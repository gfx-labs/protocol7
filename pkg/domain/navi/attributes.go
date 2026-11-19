package navi

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// Search attributes. Temporal indexes them, so navi lists one kind of
// workflow with a visibility query and finds a person's chats or an issue's
// session without asking each workflow. The namespace must have them
// registered (EnsureSearchAttributes).
var (
	// AttributeKind is the workflow's Kind, or "learning" on a learning.
	AttributeKind = temporal.NewSearchAttributeKeyKeyword("SelfdevKind")
	// AttributeStartedBy is the login of the person who started the chat.
	AttributeStartedBy = temporal.NewSearchAttributeKeyKeyword("SelfdevStartedBy")
	// AttributeOwner is the workflow that started the chat and talks to it.
	AttributeOwner = temporal.NewSearchAttributeKeyKeyword("SelfdevOwner")
	// AttributeIssue is owner/name#number of the GitHub issue the chat works on.
	AttributeIssue = temporal.NewSearchAttributeKeyKeyword("SelfdevIssue")
	// AttributeProject is the navi project. Unset for the core project.
	AttributeProject = temporal.NewSearchAttributeKeyKeyword("SelfdevProject")
	// AttributeArchived is set on a chat a person archived.
	AttributeArchived = temporal.NewSearchAttributeKeyBool("SelfdevArchived")
	// AttributeParent indexes child chats and escalations under their chat.
	AttributeParent = temporal.NewSearchAttributeKeyKeyword("SelfdevParent")
	// AttributeEscalationState is an escalation's EscalationStatus.
	AttributeEscalationState = temporal.NewSearchAttributeKeyKeyword("SelfdevEscalationState")
	// AttributeLearningState is a learning's LearningState.
	AttributeLearningState = temporal.NewSearchAttributeKeyKeyword("SelfdevLearningState")
)

// keywordAttributes are every keyword search attribute a workflow of the
// contract may carry.
var keywordAttributes = []temporal.SearchAttributeKeyKeyword{
	AttributeKind, AttributeStartedBy, AttributeOwner, AttributeIssue, AttributeProject,
	AttributeParent, AttributeEscalationState, AttributeLearningState,
}

// SearchAttributeTypes are the search attributes of the contract by name,
// with their types.
func SearchAttributeTypes() map[string]enumspb.IndexedValueType {
	attributes := make(map[string]enumspb.IndexedValueType, len(keywordAttributes)+1)
	for _, attribute := range keywordAttributes {
		attributes[attribute.GetName()] = enumspb.INDEXED_VALUE_TYPE_KEYWORD
	}
	attributes[AttributeArchived.GetName()] = enumspb.INDEXED_VALUE_TYPE_BOOL
	return attributes
}

// EnsureSearchAttributes registers the contract's search attributes in the
// namespace if they are missing. Temporal refuses a start that sets an
// attribute the namespace does not have.
func EnsureSearchAttributes(ctx context.Context, temporalClient client.Client, namespace string) error {
	operator := temporalClient.OperatorService()
	existing, err := operator.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: namespace})
	if err != nil {
		return fmt.Errorf("list the search attributes of %s: %w", namespace, err)
	}
	missing := map[string]enumspb.IndexedValueType{}
	for key, attributeType := range SearchAttributeTypes() {
		if _, ok := existing.GetCustomAttributes()[key]; !ok {
			missing[key] = attributeType
		}
	}
	if len(missing) == 0 {
		return nil
	}
	_, err = operator.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: namespace, SearchAttributes: missing})
	if err != nil {
		return fmt.Errorf("add the search attributes %v to %s: %w", missing, namespace, err)
	}
	return nil
}

// ChatWorkflowID is the workflow ID of a chat.
func ChatWorkflowID(chatID string) string { return "chat-" + chatID }

// EscalationWorkflowID is the workflow ID of an escalation.
func EscalationWorkflowID(id string) string { return "escalation-" + id }

// IssueWorkflowPrefix starts the workflow ID of every issue workflow.
const IssueWorkflowPrefix = "issue-"

// IssueWorkflowID is the workflow ID of one issue's workflow.
func IssueWorkflowID(repo string, number int) string {
	return IssueWorkflowPrefix + strings.ReplaceAll(repo, "/", "-") + "-" + strconv.Itoa(number)
}

// IssueIntakeWorkflowID is the workflow ID of an issue's intake.
func IssueIntakeWorkflowID(repo string, number int) string {
	return "intake-" + IssueWorkflowID(repo, number)
}

// ReviewWorkflowID is the workflow ID of the review of one pull request.
func ReviewWorkflowID(repo string, number int) string {
	return "review-" + strings.ReplaceAll(repo, "/", "-") + "-" + strconv.Itoa(number)
}

// LearningWorkflowPrefix starts the workflow ID of every learning.
const LearningWorkflowPrefix = "learning-"

// LearningWorkflowID is the workflow ID of a learning entry.
func LearningWorkflowID(id string) string { return LearningWorkflowPrefix + id }
