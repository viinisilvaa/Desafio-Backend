package app

const (
	FailureInsufficientFunds         = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceNotFound         = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed     = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         = "REFERENCE_MISMATCH"
	FailureReferenceKindInvalid      = "REFERENCE_KIND_INVALID"
	FailureReferenceAlreadyReversed  = "REFERENCE_ALREADY_REVERSED"
	FailureMaxRetriesExceeded        = "MAX_RETRIES_EXCEEDED"
	FailureInvalidDeadLetterMessage  = "INVALID_DLQ_MESSAGE"
	FailureDeadLetterMessageConflict = "DLQ_MESSAGE_ID_CONFLICT"
	FailureIdempotencyConflict       = "IDEMPOTENCY_CONFLICT"
)
