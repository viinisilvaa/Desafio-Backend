# Architecture

## Visão

O domínio (`internal/domain`) não importa Fx, HTTP, SQS ou PostgreSQL. `internal/app` contém casos de uso e adaptadores; `cmd/wager-service` compõe configuração, pool, OIDC, API e workers com Uber Fx. O entrypoint registra um único ciclo coordenado: abre listener, inicia workers, interrompe a entrada HTTP no shutdown, cancela e aguarda `WorkerGroup` até o deadline e fecha PostgreSQL por último. Os testes unitários verificam que todos os workers recebem cancelamento, que deadline expirado é reportado e que workers liberados podem terminar.

## Dinheiro e agregado

`Money` armazena centavos em `int64` e carrega moeda de três letras maiúsculas. Valores externos exigem string decimal com duas casas; float, notação exponencial, escala diferente e negativo são rejeitados. Aritmética detecta overflow; o zero value é inválido. A normalização para hash converte os centavos validados de volta à escala fixa, então entradas textualmente equivalentes como `0001.00` e `1.00` têm o mesmo hash. A faixa positiva máxima é `92233720368547758.07`.

Carteira é identificada por `(player_id,currency)`, inicia na versão 1 e é bloqueada com `SELECT ... FOR UPDATE` dentro do PostgreSQL. O lock é por wallet, não por processo: pools e instâncias diferentes coordenam pelo banco; wallets independentes não disputam o mesmo lock. Débito e verificação de saldo acontecem na mesma transação. `LOSS` não incrementa versão.

O saldo e o ledger são persistidos em centavos (`BIGINT`). Cada lançamento registra direção, valor, saldo anterior/posterior e versão. Checks validam a equação financeira e snapshots terminais; foreign keys compostas garantem dono/moeda da carteira e identidade da referência (carteira, jogador, moeda, rodada e provedor); constraints garantem unicidade. Triggers impedem edição do ledger e transições inválidas; constraint triggers diferidos validam direção/valor da referência, continuidade dos saldos e correspondência do saldo/versão da wallet no commit. Carteira aberta com saldo zero não cria `OPENING`; saldo positivo grava abertura, crédito, eventos e carteira num commit.

## Transação SQL

`Processor.Process` delimita uma única transação `READ COMMITTED` no repositório: inbox opcional, inserção/deduplicação da wager, lock e leitura da wallet, validação de referência, atualização do saldo, lançamento, resultado persistido e eventos outbox. A inbox e efeitos financeiros da mensagem SQS confirmam juntos. Erros transitórios fazem rollback. Rejeições definitivas confirmadas atualizam a transação e inserem evento de rejeição sem ledger. `PENDING_REFERENCE` persiste a pendência e o evento; worker retoma-a após restart.

Idempotência financeira tem duas chaves únicas persistentes, `(provider_id,idempotency_key)` e `(provider_id,external_transaction_id)`. O hash SHA-256 usa JSON de struct com ordem fixa, com provider, ID externo, jogador, wallet, rodada, jogo, tipo, valor normalizado, moeda e referência; chave de idempotência e metadados de transporte não entram. HTTP e SQS chamam o mesmo hash e caso de uso. Reutilização divergente responde conflito. Replay lê `result_balance_minor` e `wallet_version` do processamento original, não o saldo atual.

Inbox usa `(consumer_name,message_id)` e hash SHA-256 do corpo bruto do envelope; a mesma identidade com corpo alterado é conflito. A remoção SQS ocorre só após commit. Envelopes inválidos e erros transitórios permanecem para redrive; a FIFO redireciona após cinco recebimentos.

## Referências e reversões

`REFUND` deve referenciar BET processada; `ROLLBACK` pode referenciar BET, WIN ou REFUND processada e inverte seu efeito financeiro. `WIN` pode opcionalmente apontar para uma BET da mesma rodada; seu valor de payout não precisa igualar o da aposta. Provedor, jogador, wallet, moeda e rodada precisam coincidir; reversões também precisam igualar o valor referenciado. Por política, só uma reversão processada total pode apontar para cada transação original, independente de ser REFUND ou ROLLBACK; portanto refund + rollback direto da mesma aposta não é permitido. Um rollback da própria transação REFUND é possível e neutraliza o crédito.

Referência ausente ou ainda pendente grava `PENDING_REFERENCE`. O worker usa `FOR UPDATE SKIP LOCKED`, backoff exponencial limitado a 300 segundos e no máximo 20 tentativas ou 24 horas. Referência que permaneça pendente termina como `REFERENCE_NOT_PROCESSED`; referência ausente expirada termina como `REFERENCE_NOT_FOUND`. Referência terminal sem sucesso, divergente ou tipo inválido é rejeitada imediatamente com código estável. Reversão que exceda o saldo é `REVERSAL_INSUFFICIENT_FUNDS`, distinto de `INSUFFICIENT_FUNDS`.

| `failureCode` | Significado |
| --- | --- |
| `INSUFFICIENT_FUNDS` | Débito de BET excederia o saldo disponível. |
| `REVERSAL_INSUFFICIENT_FUNDS` | Reversão debitora excederia o saldo disponível. |
| `REFERENCE_NOT_FOUND` | Referência não apareceu antes de esgotar o retry/TTL. |
| `REFERENCE_NOT_PROCESSED` | Referência terminou sem sucesso ou ficou pendente até o limite. |
| `REFERENCE_MISMATCH` | Identidade, carteira, rodada, moeda ou valor incompatível. |
| `REFERENCE_KIND_INVALID` | Tipo da referência não pode ser revertido/usado por WIN. |
| `REFERENCE_ALREADY_REVERSED` | Já existe uma reversão integral processada para a referência. |

## Outbox e eventos

Outbox é gravada no mesmo commit da mudança financeira. Publishers concorrentes reclamam linhas com `FOR UPDATE SKIP LOCKED`, gravam lease de 60 segundos, enviam para `wager-events` e confirmam depois. Falha agenda backoff exponencial limitado a cinco minutos; lease vencido é recuperável. Queda após envio e antes da confirmação publica novamente o mesmo `eventId`; consumidores devem deduplicá-lo. Payloads são structs tipadas, snapshots JSON imutáveis, timestamps UTC RFC 3339 e `WalletBalanceChanged` inclui direção, valor, saldos e versão.

## Segurança

Keycloak é o IdP externo OIDC. A API descobre issuer e JWKS, valida assinatura/issuer/expiração e verifica audience `wager-api`. Não cria tokens próprios. `provider_id` do token determina escopo; o identificador enviado no body deve corresponder à identidade autenticada. Queries/replays por provedor sempre incluem `provider_id` e exigem `wager-provider`. Carteira, ledger, reconciliação e consulta global por ID exigem role `wallet-admin`, concedida ao client_credentials interno no realm local.

Em produção, use clients separados, secrets em secret manager, TLS, audience/issuer públicos estáveis e roles atribuídas por política do IdP. O usuário local `test` do LocalStack não é autorização de produção. A aplicação precisa apenas de `ReceiveMessage`, `DeleteMessage`, `GetQueueAttributes` na fila de entrada e `SendMessage` na fila de eventos; política IAM de referência fica em `iam/wager-app-policy.json`.

## Falhas e observabilidade

PostgreSQL indisponível não confirma transação e a API responde `503`; mensagens SQS não são apagadas. Indisponibilidade de SQS impede readiness e workers tentam novamente. SIGTERM cancela long polling; mensagens em processamento sem commit tornam-se visíveis novamente após o visibility timeout. Outbox conserva evento confirmado apesar da indisponibilidade de SQS.

Logs JSON incluem IDs disponíveis sem payload financeiro ou credenciais. `/metrics` expõe status, duplicatas, retries, redrive, conflitos, latência, backlog outbox e divergências de reconciliação. Liveness é independente; readiness verifica PostgreSQL e SQS.

## Limitações conhecidas

- Há um teste com três `pgxpool.Pool` independentes e outro que inicia três processos Go separados; ambos precisam de PostgreSQL e são executados pelo profile Compose `integration`.
- O profile Compose `integration` executa testes com `-race` em Keycloak/PostgreSQL/LocalStack reais, cobrindo client credentials, roles, identidade ausente/inválida, replay cruzado HTTP/SQS, dois publishers concorrentes e recuperação após `SendMessage` antes da confirmação SQL. Ainda não injeta SIGTERM do serviço completo ou reinício do banco.
- Tokens expirados, assinatura incorreta e audience são testados contra issuer/JWKS local; o teste integrado com Keycloak verifica tokens reais de dois providers, isolamento nas consultas e bloqueio de replay cruzado.
- Eventos de saída usam uma fila standard única; consumidores fazem roteamento pelo `eventType`. Métricas são locais ao processo, não agregadas entre instâncias.
- Mensagens na DLQ são registradas em `dead_letter_messages` pelo hash e metadados mínimos. Uma wager pendente só transita para `FAILED` quando provider, ID externo e hash financeiro coincidem; mensagens inválidas, sem wager ou com payload conflitante ficam auditadas sem tocar no saldo. Não é emitido evento financeiro para `FAILED`.
- O provisioning de política IAM gerenciada fica a cargo do ambiente AWS; o JSON fornecido é uma policy mínima de referência, não aplicada pelo LocalStack.
- Os testes de integração dependem do profile Compose e de `TEST_DATABASE_URL`; sem os serviços, `go test ./...` executa os unitários e marca os cenários PostgreSQL/OIDC/SQS como skipped.