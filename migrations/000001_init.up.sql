CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE wallets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id uuid NOT NULL,
    currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint NOT NULL DEFAULT 0 CHECK (balance_minor >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (player_id, currency),
    UNIQUE (id, currency),
    UNIQUE (id, player_id, currency)
);

CREATE TABLE wager_transactions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    origin text NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    provider_id text,
    external_transaction_id text,
    idempotency_key text,
    payload_hash char(64),
    wallet_id uuid NOT NULL REFERENCES wallets(id),
    player_id uuid NOT NULL,
    round_id text,
    game_id text,
    kind text NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reference_external_transaction_id text,
    reference_transaction_id uuid REFERENCES wager_transactions(id),
    status text NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code text,
    result_balance_minor bigint,
    wallet_version bigint,
    reference_attempts integer NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    reference_next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT wager_failure_code_matches_status CHECK (
        (status IN ('REJECTED', 'FAILED') AND failure_code IS NOT NULL AND failure_code <> '')
        OR (status NOT IN ('REJECTED', 'FAILED') AND failure_code IS NULL)
    ),
    CONSTRAINT wager_terminal_snapshot_required CHECK (
        status NOT IN ('PROCESSED', 'REJECTED', 'FAILED')
        OR (result_balance_minor IS NOT NULL AND result_balance_minor >= 0 AND wallet_version IS NOT NULL AND wallet_version >= 1)
    ),
    CONSTRAINT wager_origin_fields CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL AND reference_external_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING' AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT wager_loss_zero CHECK (kind <> 'LOSS' OR amount_minor = 0),
    CONSTRAINT wager_positive_amount CHECK (kind IN ('OPENING', 'LOSS') OR amount_minor > 0),
    CONSTRAINT internal_opening_positive CHECK (origin <> 'INTERNAL' OR amount_minor > 0),
    CONSTRAINT wager_reference_required CHECK (origin = 'INTERNAL' OR kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
    CONSTRAINT wager_wallet_owner_currency_fk FOREIGN KEY (wallet_id, player_id, currency) REFERENCES wallets(id, player_id, currency),
    UNIQUE (wallet_id, id),
    UNIQUE (id, wallet_id, player_id, currency, round_id, provider_id),
    CONSTRAINT wager_reference_identity_fk FOREIGN KEY (reference_transaction_id, wallet_id, player_id, currency, round_id, provider_id)
        REFERENCES wager_transactions(id, wallet_id, player_id, currency, round_id, provider_id),
    CONSTRAINT reversal_must_resolve_reference CHECK (status <> 'PROCESSED' OR kind NOT IN ('REFUND', 'ROLLBACK') OR reference_transaction_id IS NOT NULL)
);

CREATE FUNCTION protect_wager_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'terminal wager transaction is immutable' USING ERRCODE = '55000';
    END IF;
    IF ROW(NEW.origin, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
           NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id, NEW.kind, NEW.amount_minor, NEW.currency,
           NEW.reference_external_transaction_id, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.origin, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
           OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id, OLD.kind, OLD.amount_minor, OLD.currency,
           OLD.reference_external_transaction_id, OLD.created_at) THEN
        RAISE EXCEPTION 'wager transaction business fields are immutable' USING ERRCODE = '55000';
    END IF;
    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'PENDING' AND NEW.status IN ('PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')) OR
        (OLD.status = 'PENDING_REFERENCE' AND NEW.status IN ('PROCESSED', 'REJECTED', 'FAILED'))
    ) THEN
        RAISE EXCEPTION 'invalid wager transaction state transition' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wager_transaction_transition BEFORE UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION protect_wager_transaction();

CREATE UNIQUE INDEX wager_provider_idempotency_uq ON wager_transactions(provider_id, idempotency_key) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_provider_external_uq ON wager_transactions(provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_single_opening_uq ON wager_transactions(wallet_id) WHERE kind = 'OPENING';
CREATE UNIQUE INDEX wager_single_successful_reversal_uq ON wager_transactions(reference_transaction_id) WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
CREATE INDEX wager_pending_reference_idx ON wager_transactions(created_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_provider_lookup_idx ON wager_transactions(provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';

CREATE FUNCTION validate_wager_creation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.origin = 'INTERNAL' AND NEW.status <> 'PROCESSED') OR
       (NEW.origin = 'EXTERNAL' AND NEW.status <> 'PENDING') THEN
        RAISE EXCEPTION 'invalid initial transaction state for origin' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER wager_transaction_creation BEFORE INSERT ON wager_transactions FOR EACH ROW EXECUTE FUNCTION validate_wager_creation();

CREATE TABLE wallet_ledger (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id uuid NOT NULL REFERENCES wallets(id),
    transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
    direction text NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    wallet_version bigint NOT NULL CHECK (wallet_version >= 1),
    balance_before_minor bigint NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor bigint NOT NULL CHECK (balance_after_minor >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (wallet_id, transaction_id),
    UNIQUE (wallet_id, wallet_version),
    CHECK (
        (direction = 'CREDIT' AND balance_after_minor::numeric = balance_before_minor::numeric + amount_minor::numeric)
        OR
        (direction = 'DEBIT' AND balance_after_minor::numeric = balance_before_minor::numeric - amount_minor::numeric)
    ),
    FOREIGN KEY (wallet_id, currency) REFERENCES wallets(id, currency),
    FOREIGN KEY (wallet_id, transaction_id) REFERENCES wager_transactions(wallet_id, id)
);

CREATE FUNCTION reject_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet ledger is append-only' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER wallet_ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();

CREATE FUNCTION validate_committed_ledger_entry() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    transaction_kind text;
    transaction_status text;
    transaction_amount bigint;
    transaction_currency char(3);
    reference_kind text;
    expected_direction text;
BEGIN
    SELECT kind, status, amount_minor, currency INTO transaction_kind, transaction_status, transaction_amount, transaction_currency
      FROM wager_transactions WHERE id=NEW.transaction_id AND wallet_id=NEW.wallet_id;
    IF NOT FOUND OR transaction_status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry requires a processed transaction' USING ERRCODE = '23514';
    END IF;
    IF transaction_amount <> NEW.amount_minor OR transaction_currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger amount/currency differs from transaction' USING ERRCODE = '23514';
    END IF;
    CASE transaction_kind
        WHEN 'OPENING' THEN expected_direction := 'CREDIT';
        WHEN 'BET' THEN expected_direction := 'DEBIT';
        WHEN 'WIN' THEN expected_direction := 'CREDIT';
        WHEN 'REFUND' THEN expected_direction := 'CREDIT';
        WHEN 'ROLLBACK' THEN
            SELECT kind INTO reference_kind FROM wager_transactions
              WHERE id=(SELECT reference_transaction_id FROM wager_transactions WHERE id=NEW.transaction_id);
            IF reference_kind IS NULL THEN
                RAISE EXCEPTION 'rollback ledger requires a resolved reference' USING ERRCODE = '23514';
            END IF;
            IF reference_kind = 'BET' THEN expected_direction := 'CREDIT'; ELSE expected_direction := 'DEBIT'; END IF;
        ELSE
            RAISE EXCEPTION 'transaction kind cannot create a ledger entry' USING ERRCODE = '23514';
    END CASE;
    IF NEW.direction <> expected_direction THEN
        RAISE EXCEPTION 'ledger direction differs from transaction kind' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER ledger_matches_processed_transaction
  AFTER INSERT ON wallet_ledger DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION validate_committed_ledger_entry();

CREATE FUNCTION check_processed_transaction_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    entry_count bigint;
    ledger_balance bigint;
    ledger_version bigint;
    requires_entry boolean;
    reference_kind text;
    reference_status text;
    reference_amount bigint;
BEGIN
    IF NEW.status NOT IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RETURN NULL;
    END IF;
    SELECT count(*), max(balance_after_minor), max(wallet_version)
      INTO entry_count, ledger_balance, ledger_version
      FROM wallet_ledger WHERE transaction_id=NEW.id;
    requires_entry := NEW.status = 'PROCESSED' AND NEW.kind <> 'LOSS';
    IF (requires_entry AND entry_count <> 1) OR (NOT requires_entry AND entry_count <> 0) THEN
        RAISE EXCEPTION 'terminal transaction has incorrect ledger entry count' USING ERRCODE = '23514';
    END IF;
    IF entry_count = 1 AND (NEW.result_balance_minor IS DISTINCT FROM ledger_balance OR NEW.wallet_version IS DISTINCT FROM ledger_version) THEN
        RAISE EXCEPTION 'transaction result differs from ledger snapshot' USING ERRCODE = '23514';
    END IF;
    IF NEW.status = 'PROCESSED' AND (NEW.result_balance_minor IS NULL OR NEW.wallet_version IS NULL) THEN
        RAISE EXCEPTION 'processed transaction requires a persisted result' USING ERRCODE = '23514';
    END IF;
    IF NEW.status = 'PROCESSED' AND NEW.kind IN ('REFUND', 'ROLLBACK', 'WIN') AND NEW.reference_transaction_id IS NOT NULL THEN
        SELECT kind, status, amount_minor INTO reference_kind, reference_status, reference_amount
          FROM wager_transactions WHERE id=NEW.reference_transaction_id;
        IF reference_status <> 'PROCESSED' THEN
            RAISE EXCEPTION 'processed operation requires a processed reference' USING ERRCODE = '23514';
        END IF;
        IF NEW.kind = 'WIN' AND reference_kind <> 'BET' THEN
            RAISE EXCEPTION 'WIN reference must be a BET' USING ERRCODE = '23514';
        END IF;
        IF NEW.kind = 'REFUND' AND (reference_kind <> 'BET' OR reference_amount <> NEW.amount_minor) THEN
            RAISE EXCEPTION 'REFUND must fully match a processed BET' USING ERRCODE = '23514';
        END IF;
        IF NEW.kind = 'ROLLBACK' AND (reference_kind NOT IN ('BET', 'WIN', 'REFUND') OR reference_amount <> NEW.amount_minor) THEN
            RAISE EXCEPTION 'ROLLBACK must fully match an eligible processed transaction' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wager_ledger_terminal_consistency
  AFTER INSERT OR UPDATE ON wager_transactions DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION check_processed_transaction_ledger();

CREATE FUNCTION check_wallet_ledger_balance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_wallet uuid;
    stored_balance bigint;
    stored_version bigint;
    ledger_balance bigint;
    ledger_version bigint;
    previous_balance bigint;
    previous_version bigint;
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN
        target_wallet := NEW.id;
    ELSE
        target_wallet := NEW.wallet_id;
    END IF;
    SELECT balance_minor, version INTO stored_balance, stored_version FROM wallets WHERE id=target_wallet;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT balance_after_minor, wallet_version INTO ledger_balance, ledger_version
      FROM wallet_ledger WHERE wallet_id=target_wallet ORDER BY wallet_version DESC LIMIT 1;
    IF NOT FOUND THEN
        ledger_balance := 0;
        ledger_version := 1;
    END IF;
    IF EXISTS (
        SELECT 1 FROM (
            SELECT wallet_version, balance_before_minor,
                   lag(balance_after_minor) OVER (ORDER BY wallet_version) AS previous_balance,
                   lag(wallet_version) OVER (ORDER BY wallet_version) AS previous_version
              FROM wallet_ledger WHERE wallet_id=target_wallet
        ) entries
        WHERE (previous_version IS NULL AND (wallet_version NOT IN (1,2) OR balance_before_minor <> 0))
           OR (previous_version IS NOT NULL AND (wallet_version <> previous_version + 1 OR balance_before_minor <> previous_balance))
    ) THEN
        RAISE EXCEPTION 'wallet ledger entries are not a continuous balance chain' USING ERRCODE = '23514';
    END IF;
    IF stored_balance <> ledger_balance OR stored_version <> ledger_version THEN
        RAISE EXCEPTION 'wallet balance/version does not match append-only ledger' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wallet_balance_matches_ledger
  AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger_balance();
CREATE CONSTRAINT TRIGGER ledger_matches_wallet
  AFTER INSERT ON wallet_ledger DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger_balance();

CREATE TABLE inbox (
    consumer_name text NOT NULL,
    message_id text NOT NULL,
    payload_hash char(64) NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox (
    event_id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text,
    occurred_at timestamptz NOT NULL,
    version bigint NOT NULL,
    payload jsonb NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    published_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending_idx ON outbox(next_attempt_at, created_at) WHERE published_at IS NULL;

CREATE TABLE dead_letter_messages (
    queue_name text NOT NULL,
    message_id text NOT NULL,
    payload_hash char(64) NOT NULL,
    provider_id text,
    external_transaction_id text,
    transaction_id uuid REFERENCES wager_transactions(id),
    failure_code text NOT NULL,
    disposition text NOT NULL CHECK (disposition IN ('FAILED', 'TERMINAL', 'NO_TRANSACTION', 'INVALID_MESSAGE', 'CONFLICTING_PAYLOAD')),
    received_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_name, message_id)
);