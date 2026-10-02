# Wager Service

Serviço HTTP em Go para processar transações de apostas e movimentações de carteira. O projeto prioriza consistência financeira, idempotência, concorrência segura e processamento assíncrono resiliente.

## Tecnologias

- Go 1.23 ou superior
- PostgreSQL 17
- AWS SQS (LocalStack no ambiente local)
- Keycloak e OpenID Connect
- Uber Fx para composição e ciclo de vida

## Componentes

- `internal/domain`: valores monetários e regras de transação e carteira
- `internal/app`: casos de uso, API HTTP, autenticação, filas e workers
- `cmd/wager-service`: composição e inicialização do serviço
- `cmd/migrate` e `migrations`: aplicação e reversão do schema
- `keycloak/realm.json`: realm local com clients e roles de teste
- `localstack/init`: criação automática das filas SQS locais

As decisões, invariantes, interpretações e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Pré-requisitos

- Docker com o plugin Docker Compose
- Go 1.23+ para executar testes no host
- `curl`, `jq` e `uuidgen` para seguir os exemplos HTTP
- Um compilador C para `go test -race` no host; os testes integrados pelo Compose usam a imagem Go do serviço de teste

## Preparar e iniciar

Clone o repositório e entre na pasta do projeto. O arquivo `.env.example` contém somente valores locais de exemplo. O Compose tem os mesmos valores como padrão; copie o arquivo apenas se quiser configurar valores locais próprios:

```sh
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Na inicialização, o LocalStack cria as filas de entrada, eventos e dead letter; o Keycloak importa o realm `wager`; e o container da aplicação executa `migrate up` antes de iniciar o serviço. A API fica em `http://localhost:8082` e o Keycloak em `http://localhost:8080`.

Confirme a inicialização:

```sh
curl -i http://localhost:8082/health/live
curl -i http://localhost:8082/health/ready
```

Para parar os serviços sem apagar os dados do PostgreSQL:

```sh
docker compose down
```

Para apagar também o volume local do banco e começar do zero:

```sh
docker compose down -v
```

Isso remove os dados persistidos localmente. Não use credenciais do `.env.example` fora de desenvolvimento.

## Variáveis locais

O Compose aceita estas variáveis, com os valores abaixo como padrão:

| Variável | Padrão local | Uso |
| --- | --- | --- |
| `POSTGRES_USER` | `wager` | Usuário do PostgreSQL |
| `POSTGRES_PASSWORD` | `wager-local-only` | Senha local do PostgreSQL |
| `KEYCLOAK_ADMIN` | `admin` | Administrador local do Keycloak |
| `KEYCLOAK_ADMIN_PASSWORD` | `admin-local-only` | Senha local do Keycloak |
| `AWS_ACCESS_KEY_ID` | `test` | Credencial fictícia do LocalStack |
| `AWS_SECRET_ACCESS_KEY` | `test` | Credencial fictícia do LocalStack |
| `AWS_REGION` | `us-east-1` | Região usada pelo LocalStack e SDK |

O serviço também pode ser configurado diretamente por `HTTP_ADDR`, `DATABASE_URL`, `OIDC_ISSUER_URL`, `OIDC_AUDIENCE`, `AWS_ENDPOINT_URL`, `SQS_REQUEST_QUEUE_URL`, `SQS_EVENT_QUEUE_URL` e `SQS_DLQ_QUEUE_URL`. Os endereços internos usados pelo Compose estão definidos em `docker-compose.yml`.

## Autenticação e chamadas HTTP

O realm local cria automaticamente os clients `wager-provider-a`, `wager-provider-b` e `wager-internal`, com credenciais locais declaradas em `keycloak/realm.json`. Os clients de provedor recebem a role `wager-provider`; o client interno recebe `wallet-admin`. O token é obtido pelo fluxo OAuth2 `client_credentials`:

```sh
BASE_URL=http://localhost:8082
OIDC_URL=http://localhost:8080/realms/wager/protocol/openid-connect/token

PROVIDER_TOKEN=$(curl -fsS "$OIDC_URL" \
  -d grant_type=client_credentials \
  -d client_id=wager-provider-a \
  -d client_secret=provider-a-local-secret | jq -r .access_token)

INTERNAL_TOKEN=$(curl -fsS "$OIDC_URL" \
  -d grant_type=client_credentials \
  -d client_id=wager-internal \
  -d client_secret=internal-local-secret | jq -r .access_token)
```

Crie uma carteira usando o token interno e guarde os identificadores retornados:

```sh
PLAYER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
WALLET=$(curl -fsS "$BASE_URL/wallets" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER_ID\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
WALLET_ID=$(printf '%s' "$WALLET" | jq -r .id)
```

Envie uma aposta com o token do provedor. O `providerId` do corpo precisa corresponder ao `provider_id` do token; a chave de idempotência é obrigatória:

```sh
EXTERNAL_ID=$(uuidgen)
curl -i "$BASE_URL/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $EXTERNAL_ID" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$EXTERNAL_ID\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}"
```

Uma chamada repetida com os mesmos dados retorna o resultado original. Uma chave reutilizada com dados diferentes é conflito. O serviço também oferece:

- `GET /wallets/{walletID}`: consulta de carteira, role `wallet-admin`
- `GET /wallets/{walletID}/ledger`: consulta paginada do ledger, role `wallet-admin`
- `POST /wallets/{walletID}/reconciliation`: reconciliação, role `wallet-admin`
- `GET /providers/{providerID}/wagering/transactions/{externalTransactionID}`: consulta restrita ao próprio provedor
- `GET /wagering/transactions/{transactionID}`: consulta pelo identificador interno
- `GET /health/live`, `GET /health/ready` e `GET /metrics`: liveness, readiness e métricas

## Consumidor SQS

Ao iniciar, o LocalStack executa `localstack/init/10-create-queues.sh` e provisiona `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events`. A fila de transações usa redrive para a DLQ após cinco recebimentos sem confirmação; mensagens válidas são removidas somente depois do commit no PostgreSQL. Mensagens inválidas ou com falha transitória ficam na fila para nova tentativa.

O corpo da mensagem deve seguir este envelope. Use `PLAYER_ID` e `WALLET_ID` de uma carteira existente, como a criada no exemplo HTTP acima:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "PLAYER_ID",
    "walletId": "WALLET_ID",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

Para publicar a mensagem no LocalStack, gere IDs novos e envie à fila com os atributos obrigatórios de FIFO. A carteira deve ter saldo suficiente para o débito:

```sh
MESSAGE_ID=$(uuidgen)
EXTERNAL_ID=$(uuidgen)
ENVELOPE=$(jq -n \
  --arg messageId "$MESSAGE_ID" \
  --arg occurredAt "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" \
  --arg playerId "$PLAYER_ID" \
  --arg walletId "$WALLET_ID" \
  --arg externalId "$EXTERNAL_ID" \
  '{messageId:$messageId,type:"WagerTransactionRequested",occurredAt:$occurredAt,data:{providerId:"provider-a",externalTransactionId:$externalId,idempotencyKey:("provider-a:" + $externalId),playerId:$playerId,walletId:$walletId,roundId:"round-987",gameId:"fortune-chimp",kind:"BET",money:{amount:"25.00",currency:"BRL"}}}')

docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-body "$ENVELOPE" \
  --message-group-id "$WALLET_ID" \
  --message-deduplication-id "$MESSAGE_ID"
```

O consumidor verifica o tipo e o timestamp do envelope, valida a transação e usa `(consumer, messageId)` junto ao hash do corpo para deduplicar. `MessageGroupId` preserva a ordem por carteira; `MessageDeduplicationId` é obrigatório porque a fila não usa deduplicação baseada no conteúdo.

## Migrações

O container da aplicação aplica automaticamente as migrações pendentes ao iniciar. Para operar manualmente, com o serviço em execução:

```sh
docker compose exec app migrate up
docker compose exec app migrate down
```

`migrate down` reverte somente a migração mais recente. A migração inicial remove objetos do schema; faça backup antes de reverter dados que precisem ser preservados.

## Testes e qualidade

Na raiz do repositório, execute a suíte padrão e as verificações estáticas:

```sh
go test ./...
go vet ./...
gofmt -d $(git ls-files '*.go')
```

`gofmt -d` não deve imprimir arquivos. Para formatar os arquivos Go do repositório, use `gofmt -w $(git ls-files '*.go')`. A suíte padrão inclui testes unitários; testes que dependem de PostgreSQL, Keycloak ou LocalStack são ignorados quando suas variáveis de integração não estão configuradas.

Para executar testes com o detector de corrida no host, instale também um compilador C:

```sh
go test -race ./...
```

O teste de concorrência com um PostgreSQL local pode ser executado isoladamente:

```sh
docker compose up -d postgres
TEST_DATABASE_URL='postgres://wager:wager-local-only@localhost:5432/wager?sslmode=disable' \
  go test -race ./internal/app -run 'TestIndependentProcessorsPreserveWalletInvariants|TestThreeIndependentProcessesCompeteForWallet' -count=1
```

Para os testes integrados com PostgreSQL, Keycloak e LocalStack reais, execute o profile dedicado:

```sh
docker compose --profile integration run --rm integration-tests
```

Esse comando inicia as dependências necessárias e executa os cenários de autenticação OIDC/HTTP/SQS e idempotência cruzada, concorrência entre pools e três processos Go independentes, auditoria de DLQ e recuperação da outbox após publicação sem confirmação no banco. Os próprios testes aplicam as migrações. Esse perfil testa falhas controladas nesses componentes; não é um teste de carga ou de implantação em AWS.

Não há build tags Go no projeto. Os testes integrados são selecionados pelo profile e pelos nomes de teste configurados em `docker-compose.yml`.

## Limitações e escopo

O serviço usa LocalStack para SQS localmente; a criação de IAM na AWS é responsabilidade do ambiente e a policy em `iam/wager-app-policy.json` é uma referência mínima. TLS público, armazenamento de secrets e configuração de produção do IdP não são provisionados por este repositório. O profile de integração não simula SIGTERM do serviço completo nem reinício do PostgreSQL; os limites e interpretações restantes estão detalhados em [ARCHITECTURE.md](ARCHITECTURE.md).

## Referências

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [migrations/000001_init.up.sql](migrations/000001_init.up.sql)
- [internal/app/processor.go](internal/app/processor.go)
- [internal/domain/wager.go](internal/domain/wager.go)
