package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/desafio/wager-service/internal/domain"
	"github.com/desafio/wager-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOIDCAndHTTPToSQSIdempotency(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	issuerURL := os.Getenv("TEST_OIDC_ISSUER_URL")
	endpointURL := os.Getenv("TEST_SQS_ENDPOINT_URL")
	if databaseURL == "" || issuerURL == "" || endpointURL == "" {
		t.Skip("set TEST_DATABASE_URL, TEST_OIDC_ISSUER_URL, and TEST_SQS_ENDPOINT_URL to run real-service integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL, "up"); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	metrics := NewMetrics()
	processor := NewProcessor(pool, metrics)
	auth, err := NewAuthenticator(Config{OIDCIssuerURL: issuerURL, OIDCAudience: env("TEST_OIDC_AUDIENCE", "wager-api")})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := &HTTPServer{processor: processor, auth: auth, pool: pool, logger: logger, metrics: metrics}

	providerID := env("TEST_PROVIDER_CLIENT_ID", "wager-provider-a")
	providerSecret := env("TEST_PROVIDER_CLIENT_SECRET", "provider-a-local-secret")
	providerBID := env("TEST_PROVIDER_B_CLIENT_ID", "wager-provider-b")
	providerBSecret := env("TEST_PROVIDER_B_CLIENT_SECRET", "provider-b-local-secret")
	internalID := env("TEST_INTERNAL_CLIENT_ID", "wager-internal")
	internalSecret := env("TEST_INTERNAL_CLIENT_SECRET", "internal-local-secret")
	providerToken := integrationClientToken(ctx, t, issuerURL, providerID, providerSecret)
	providerBToken := integrationClientToken(ctx, t, issuerURL, providerBID, providerBSecret)
	internalToken := integrationClientToken(ctx, t, issuerURL, internalID, internalSecret)
	if _, err = integrationClientTokenMaybe(ctx, issuerURL, providerID, "incorrect-secret"); err == nil {
		t.Fatal("Keycloak accepted invalid client credentials")
	}
	providerPrincipal := authenticateIntegrationToken(t, auth, providerToken)
	if providerPrincipal.ProviderID != "provider-a" || !providerPrincipal.Roles["wager-provider"] || providerPrincipal.Roles["wallet-admin"] {
		t.Fatalf("unexpected provider principal: %+v", providerPrincipal)
	}
	providerBPrincipal := authenticateIntegrationToken(t, auth, providerBToken)
	if providerBPrincipal.ProviderID != "provider-b" || !providerBPrincipal.Roles["wager-provider"] || providerBPrincipal.Roles["wallet-admin"] {
		t.Fatalf("unexpected provider B principal: %+v", providerBPrincipal)
	}
	internalPrincipal := authenticateIntegrationToken(t, auth, internalToken)
	if internalPrincipal.ProviderID != "internal" || !internalPrincipal.Roles["wallet-admin"] || internalPrincipal.Roles["wager-provider"] {
		t.Fatalf("unexpected internal principal: %+v", internalPrincipal)
	}
	for _, header := range []string{"", "Bearer not.a.valid.token"} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		if header != "" {
			request.Header.Set("Authorization", header)
		}
		if _, err := auth.Authenticate(request); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("invalid auth header %q returned %v", header, err)
		}
	}

	playerID := uuid.NewString()
	initial, _ := domain.ParseMoney("100.00", "BRL")
	createBody, _ := json.Marshal(struct {
		PlayerID       string       `json:"playerId"`
		InitialBalance domain.Money `json:"initialBalance"`
	}{PlayerID: playerID, InitialBalance: initial})
	providerWalletRequest := httptest.NewRequest(http.MethodPost, "/wallets", strings.NewReader(string(createBody)))
	providerWalletRequest.Header.Set("Authorization", "Bearer "+providerToken)
	providerWalletResponse := httptest.NewRecorder()
	server.createWallet(providerWalletResponse, providerWalletRequest)
	if providerWalletResponse.Code != http.StatusForbidden {
		t.Fatalf("provider wallet creation status=%d, want 403", providerWalletResponse.Code)
	}
	internalWalletRequest := httptest.NewRequest(http.MethodPost, "/wallets", strings.NewReader(string(createBody)))
	internalWalletRequest.Header.Set("Authorization", "Bearer "+internalToken)
	internalWalletResponse := httptest.NewRecorder()
	server.createWallet(internalWalletResponse, internalWalletRequest)
	if internalWalletResponse.Code != http.StatusCreated {
		t.Fatalf("internal wallet creation status=%d body=%s", internalWalletResponse.Code, internalWalletResponse.Body.String())
	}
	var wallet WalletView
	if err = json.Unmarshal(internalWalletResponse.Body.Bytes(), &wallet); err != nil {
		t.Fatal(err)
	}

	externalID := "cross-channel-" + uuid.NewString()
	idempotencyKey := "provider-a:" + externalID
	amount, _ := domain.ParseMoney("25.00", "BRL")
	requestData := domain.WagerRequest{ProviderID: "provider-a", ExternalTransactionID: externalID, IdempotencyKey: idempotencyKey, PlayerID: playerID, WalletID: wallet.ID, RoundID: "round-1", GameID: "game-1", Kind: domain.KindBet, Money: amount}
	body, err := json.Marshal(struct {
		ProviderID                     string           `json:"providerId"`
		ExternalTransactionID          string           `json:"externalTransactionId"`
		PlayerID                       string           `json:"playerId"`
		WalletID                       string           `json:"walletId"`
		RoundID                        string           `json:"roundId"`
		GameID                         string           `json:"gameId"`
		Kind                           domain.WagerKind `json:"kind"`
		Money                          domain.Money     `json:"money"`
		ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId,omitempty"`
	}{requestData.ProviderID, requestData.ExternalTransactionID, requestData.PlayerID, requestData.WalletID, requestData.RoundID, requestData.GameID, requestData.Kind, requestData.Money, requestData.ReferenceExternalTransactionID})
	if err != nil {
		t.Fatal(err)
	}
	postWager := func(token string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(string(body)))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Idempotency-Key", idempotencyKey)
		response := httptest.NewRecorder()
		server.submitWager(response, request)
		return response
	}
	if response := postWager(internalToken); response.Code != http.StatusForbidden {
		t.Fatalf("internal client wager status=%d, want 403", response.Code)
	}
	if response := postWager(""); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated wager status=%d, want 401", response.Code)
	}
	firstResponse := postWager(providerToken)
	if firstResponse.Code != http.StatusCreated {
		t.Fatalf("provider wager status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	var original Result
	if err = json.Unmarshal(firstResponse.Body.Bytes(), &original); err != nil {
		t.Fatal(err)
	}
	replayResponse := postWager(providerToken)
	var replay Result
	if replayResponse.Code != http.StatusOK || json.Unmarshal(replayResponse.Body.Bytes(), &replay) != nil || !replay.IdempotentReplay || replay.Balance.MinorUnits() != original.Balance.MinorUnits() {
		t.Fatalf("HTTP replay did not return the original result: status=%d body=%s", replayResponse.Code, replayResponse.Body.String())
	}
	if response := postWager(providerBToken); response.Code != http.StatusForbidden {
		t.Fatalf("provider B replay status=%d, want 403", response.Code)
	}
	otherProviderRequest := httptest.NewRequest(http.MethodGet, "/providers/provider-b/wagering/transactions/"+externalID, nil)
	otherProviderRequest.Header.Set("Authorization", "Bearer "+providerToken)
	otherProviderRequest.SetPathValue("providerID", "provider-b")
	otherProviderRequest.SetPathValue("externalTransactionID", externalID)
	otherProviderResponse := httptest.NewRecorder()
	server.getProviderTransaction(otherProviderResponse, otherProviderRequest)
	if otherProviderResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-provider query status=%d, want 403", otherProviderResponse.Code)
	}
	providerScopedQuery := httptest.NewRequest(http.MethodGet, "/wagering/transactions/"+original.TransactionID, nil)
	providerScopedQuery.Header.Set("Authorization", "Bearer "+providerBToken)
	providerScopedQuery.SetPathValue("transactionID", original.TransactionID)
	providerScopedResponse := httptest.NewRecorder()
	server.getTransactionByID(providerScopedResponse, providerScopedQuery)
	if providerScopedResponse.Code != http.StatusNotFound {
		t.Fatalf("provider B internal-ID query status=%d, want scoped 404", providerScopedResponse.Code)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(env("AWS_REGION", "us-east-1")),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(env("AWS_ACCESS_KEY_ID", "test"), env("AWS_SECRET_ACCESS_KEY", "test"), "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpointURL) })
	queueName := "wager-integration-" + strings.ReplaceAll(uuid.NewString(), "-", "") + ".fifo"
	queueOutput, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName), Attributes: map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "45",
	}})
	if err != nil {
		t.Fatal(err)
	}
	queueURL := aws.ToString(queueOutput.QueueUrl)
	defer func() {
		_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	}()
	consumer := NewSQSConsumer(sqsClient, Config{SQSRequestQueueURL: queueURL}, processor, logger, metrics)
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	consumerDone := make(chan struct{})
	go func() { defer close(consumerDone); consumer.Run(consumerCtx) }()
	defer func() {
		stopConsumer()
		select {
		case <-consumerDone:
		case <-time.After(3 * time.Second):
			t.Error("SQS consumer did not stop after cancellation")
		}
	}()
	messageID := uuid.NewString()
	envelope := requestEnvelope{MessageID: messageID, Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Data: requestData}
	messageBody, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageGroupId: aws.String(wallet.ID), MessageDeduplicationId: aws.String(externalID), MessageBody: aws.String(string(messageBody))}); err != nil {
		t.Fatal(err)
	}
	for {
		var completed bool
		if err = pool.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox WHERE consumer_name=$1 AND message_id=$2`, inboxConsumerName, messageID).Scan(&completed); err == nil && completed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SQS message was not committed to inbox: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	for {
		attributes, attrErr := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
		if attrErr == nil && attributes.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] == "0" && attributes.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] == "0" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SQS message was committed but not acknowledged: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	finalWallet, err := processor.GetWallet(ctx, wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalWallet.Balance.MinorUnits() != 7500 || finalWallet.Version != 2 {
		t.Fatalf("HTTP+SQS replay caused duplicate movement: balance=%d version=%d", finalWallet.Balance.MinorUnits(), finalWallet.Version)
	}

	eventQueueName := "wager-events-integration-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	eventQueueOutput, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(eventQueueName)})
	if err != nil {
		t.Fatal(err)
	}
	eventQueueURL := aws.ToString(eventQueueOutput.QueueUrl)
	defer func() {
		_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(eventQueueURL)})
	}()
	publisherConfig := Config{SQSEventQueueURL: eventQueueURL}
	publishers := []*OutboxPublisher{
		NewOutboxPublisher(pool, sqsClient, publisherConfig, logger, metrics),
		NewOutboxPublisher(pool, sqsClient, publisherConfig, logger, metrics),
	}
	publisherCtx, stopPublishers := context.WithCancel(ctx)
	publishersDone := make(chan struct{})
	go func() {
		var group sync.WaitGroup
		for _, publisher := range publishers {
			group.Add(1)
			go func(publisher *OutboxPublisher) {
				defer group.Done()
				publisher.Run(publisherCtx)
			}(publisher)
		}
		group.Wait()
		close(publishersDone)
	}()
	defer func() {
		stopPublishers()
		select {
		case <-publishersDone:
		case <-time.After(3 * time.Second):
			t.Error("outbox publishers did not stop after cancellation")
		}
	}()
	eventIDs := make(map[string]bool)
	observedTypes := make(map[string]int)
	for observedTypes["WagerTransactionProcessed"]+observedTypes["WalletBalanceChanged"] < 4 {
		response, receiveErr := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(eventQueueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		for _, message := range response.Messages {
			var event eventEnvelope
			if err = json.Unmarshal([]byte(aws.ToString(message.Body)), &event); err != nil {
				t.Fatal(err)
			}
			if event.AggregateID == wallet.ID {
				if event.EventID == "" || eventIDs[event.EventID] {
					t.Fatalf("missing or repeated eventId %q", event.EventID)
				}
				eventIDs[event.EventID] = true
				observedTypes[event.EventType]++
			}
			if _, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(eventQueueURL), ReceiptHandle: message.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("outbox publishers did not publish the wallet snapshot: %v", ctx.Err())
		default:
		}
	}
	if observedTypes["WagerTransactionProcessed"] != 2 || observedTypes["WalletBalanceChanged"] != 2 {
		t.Fatalf("unexpected event counts for wallet: %+v", observedTypes)
	}
}

func integrationClientToken(ctx context.Context, t *testing.T, issuer, clientID, secret string) string {
	t.Helper()
	token, err := integrationClientTokenMaybe(ctx, issuer, clientID, secret)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func integrationClientTokenMaybe(ctx context.Context, issuer, clientID, secret string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(issuer, "/")+"/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.SetBasicAuth(clientID, secret)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("client credentials grant returned HTTP %d", response.StatusCode)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err = json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "", err
	}
	if payload.AccessToken == "" {
		return "", fmt.Errorf("identity provider returned an empty access token")
	}
	return payload.AccessToken, nil
}

func authenticateIntegrationToken(t *testing.T, auth *Authenticator, value string) Principal {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+value)
	principal, err := auth.Authenticate(request)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}
