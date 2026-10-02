package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestFIFOMessageBatchGroupsPreservePerWalletOrder(t *testing.T) {
	messages := []types.Message{
		{MessageId: aws.String("a-1"), Attributes: map[string]string{"MessageGroupId": "wallet-a"}},
		{MessageId: aws.String("b-1"), Attributes: map[string]string{"MessageGroupId": "wallet-b"}},
		{MessageId: aws.String("a-2"), Attributes: map[string]string{"MessageGroupId": "wallet-a"}},
		{MessageId: aws.String("a-3"), Attributes: map[string]string{"MessageGroupId": "wallet-a"}},
		{MessageId: aws.String("b-2"), Attributes: map[string]string{"MessageGroupId": "wallet-b"}},
	}
	groups := groupFIFOMessageBatch(messages)
	if len(groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(groups))
	}
	got := make(map[string][]string)
	for _, group := range groups {
		for _, message := range group {
			groupID := message.Attributes["MessageGroupId"]
			got[groupID] = append(got[groupID], aws.ToString(message.MessageId))
		}
	}
	if len(got["wallet-a"]) != 3 || got["wallet-a"][0] != "a-1" || got["wallet-a"][1] != "a-2" || got["wallet-a"][2] != "a-3" {
		t.Fatalf("wallet-a order was not preserved: %v", got["wallet-a"])
	}
	if len(got["wallet-b"]) != 2 || got["wallet-b"][0] != "b-1" || got["wallet-b"][1] != "b-2" {
		t.Fatalf("wallet-b order was not preserved: %v", got["wallet-b"])
	}
}
