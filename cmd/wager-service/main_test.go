package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/desafio/wager-service/internal/app"
	"github.com/desafio/wager-service/migrations"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

func TestFxDependencyGraph(t *testing.T) {
	err := fx.ValidateApp(
		fx.Module("wager-service",
			fx.Provide(
				app.NewConfig,
				app.NewMetrics,
				newLogger,
				newPool,
				newSQSClient,
				app.NewAuthenticator,
				app.NewProcessor,
				app.NewHTTPServer,
				app.NewSQSConsumer,
				app.NewOutboxPublisher,
				app.NewReferenceWorker,
				app.NewDLQAuditor,
			),
			fx.Invoke(validateDependencies, registerLifecycle),
		),
	)
	if err != nil {
		t.Fatalf("Fx dependency graph is invalid: %v", err)
	}
}

func TestServiceGracefulSIGTERM(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	issuerURL := os.Getenv("TEST_OIDC_ISSUER_URL")
	sqsEndpoint := os.Getenv("TEST_SQS_ENDPOINT_URL")
	if databaseURL == "" || issuerURL == "" || sqsEndpoint == "" {
		t.Skip("set TEST_DATABASE_URL, TEST_OIDC_ISSUER_URL, and TEST_SQS_ENDPOINT_URL to run service SIGTERM integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL, "up"); err != nil {
		t.Fatal(err)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(integrationEnv("AWS_REGION", "us-east-1")),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(integrationEnv("AWS_ACCESS_KEY_ID", "test"), integrationEnv("AWS_SECRET_ACCESS_KEY", "test"), "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg, func(options *sqs.Options) { options.BaseEndpoint = aws.String(sqsEndpoint) })
	queueSuffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	queueNames := []struct {
		name       string
		attributes map[string]string
	}{
		{name: "wager-sigterm-" + queueSuffix + ".fifo", attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}},
		{name: "wager-sigterm-dlq-" + queueSuffix + ".fifo", attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}},
		{name: "wager-sigterm-events-" + queueSuffix},
	}
	queueURLs := make([]string, 0, len(queueNames))
	for _, queue := range queueNames {
		output, createErr := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queue.name), Attributes: queue.attributes})
		if createErr != nil {
			t.Fatal(createErr)
		}
		queueURLs = append(queueURLs, aws.ToString(output.QueueUrl))
	}
	defer func() {
		for _, queueURL := range queueURLs {
			_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
		}
	}()

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate service source directory")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../.."))
	binaryPath := filepath.Join(t.TempDir(), "wager-service")
	build := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/wager-service")
	build.Dir = repositoryRoot
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build service: %v\n%s", buildErr, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	command := exec.CommandContext(ctx, binaryPath)
	command.Dir = repositoryRoot
	command.Env = replaceEnvironment(os.Environ(), map[string]string{
		"HTTP_ADDR":             address,
		"DATABASE_URL":          databaseURL,
		"OIDC_ISSUER_URL":       issuerURL,
		"OIDC_AUDIENCE":         integrationEnv("TEST_OIDC_AUDIENCE", "wager-api"),
		"AWS_REGION":            integrationEnv("AWS_REGION", "us-east-1"),
		"AWS_ACCESS_KEY_ID":     integrationEnv("AWS_ACCESS_KEY_ID", "test"),
		"AWS_SECRET_ACCESS_KEY": integrationEnv("AWS_SECRET_ACCESS_KEY", "test"),
		"AWS_ENDPOINT_URL":      sqsEndpoint,
		"SQS_REQUEST_QUEUE_URL": queueURLs[0],
		"SQS_DLQ_QUEUE_URL":     queueURLs[1],
		"SQS_EVENT_QUEUE_URL":   queueURLs[2],
	})
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()
	t.Cleanup(func() {
		if command.ProcessState != nil {
			return
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-waitResult:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-waitResult
		}
	})

	client := &http.Client{Timeout: 500 * time.Millisecond}
	startupDeadline := time.NewTimer(20 * time.Second)
	defer startupDeadline.Stop()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		response, requestErr := client.Get("http://" + address + "/health/live")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case processErr := <-waitResult:
			t.Fatalf("service exited before becoming live: %v\nstdout:\n%s\nstderr:\n%s", processErr, stdout.String(), stderr.String())
		case <-startupDeadline.C:
			t.Fatalf("service did not become live\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
		case <-poll.C:
		case <-ctx.Done():
			t.Fatalf("service startup context ended: %v", ctx.Err())
		}
	}
	if err = command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case processErr := <-waitResult:
		if processErr != nil {
			t.Fatalf("service exited with error after SIGTERM: %v\nstdout:\n%s\nstderr:\n%s", processErr, stdout.String(), stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = command.Process.Kill()
		t.Fatalf("service did not stop after SIGTERM\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "service stopped") {
		t.Fatalf("shutdown completion was not observable\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
}

func replaceEnvironment(environment []string, overrides map[string]string) []string {
	result := make([]string, 0, len(environment)+len(overrides))
	for _, entry := range environment {
		key, _, found := strings.Cut(entry, "=")
		if !found {
			result = append(result, entry)
			continue
		}
		if _, replaced := overrides[key]; !replaced {
			result = append(result, entry)
		}
	}
	for key, value := range overrides {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	return result
}

func integrationEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
