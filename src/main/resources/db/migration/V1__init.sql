-- Local service state. NOTE: no outbox / event table. Temporal owns workflow state.

CREATE TABLE orders (
    order_id       VARCHAR(64)   PRIMARY KEY,
    saga_id        VARCHAR(64)   NOT NULL UNIQUE,
    customer_id    VARCHAR(64)   NOT NULL,
    amount         NUMERIC(19,4) NOT NULL,
    currency       CHAR(3)       NOT NULL,
    status         VARCHAR(32)   NOT NULL,
    failure_reason TEXT,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Idempotency ledger shared by all activities and message handlers.
CREATE TABLE idempotency_record (
    idem_key     VARCHAR(200) PRIMARY KEY,           -- sagaId:stepName
    status       VARCHAR(16)  NOT NULL,              -- IN_PROGRESS | COMPLETED
    result       TEXT,                               -- JSON of the step result
    locked_until TIMESTAMPTZ  NOT NULL,              -- lease for crashed attempts
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
