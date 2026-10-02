# Wager Service

## Visão geral

O Wager Service é uma aplicação em Go para processar transações de apostas e carteira com foco em consistência financeira, concorrência segura e idempotência. O sistema foi desenhado para manter invariantes de saldo, registrar todas as movimentações em um ledger append-only e garantir que eventos e mensagens não causem duplicações ou inconsistências em cenários reais de produção.

A implementação combina PostgreSQL para persistência transacional, fila SQS para integração assíncrona e autenticação OIDC para autorização por provedor e role interna.

## Objetivos do projeto

- manutenir saldo da carteira consistente sob concorrência
- impedir reprocessamento duplicado de transações
- registrar cada operação em ledger imutável
- suportar transações de aposta, ganho, reembolso e rollback
- publicar eventos de negócio com outbox para garantir entrega confiável
- lidar com filas e DLQ com auditoria e recuperação de falhas
- permitir autenticação e autorização de clientes externos e internos

## Stack técnica

- Language: Go 1.23+
- Database: PostgreSQL
- Messaging: AWS SQS via SDK v2
- Auth: OIDC / Keycloak
- Runtime: Uber Fx
- Tests: Go testing

## Arquitetura

O projeto segue uma separação clara entre domínio e infraestrutura:

- `internal/domain`: regras do domínio, valores monetários, regras de carteira e transações
- `internal/app`: casos de uso, adaptadores, processadores, workers e HTTP handlers
- `cmd/wager-service`: composição da aplicação e lifecycle
- `migrations`: schema do banco e controles de integridade

Principais componentes:

- `Processor`: núcleo de processamento financeiro
- `HTTPServer`: API HTTP protegida por autenticação OIDC
- `SQSConsumer`: consumo de mensagens de transações
- `OutboxPublisher`: publicação de eventos de saída
- `ReferenceWorker`: processamento de referências pendentes
- `DLQAuditor`: auditoria e recuperação de mensagens em dead letter

## Regras de negócio principais

- Saldo e versão da carteira são atualizados dentro de uma mesma transação
- Cada carteira tem uma chave única por jogador e moeda
- A operação é idempotente por `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)`
- O ledger é append-only e valida continuidade do saldo
- Rejeições de negócio são persistidas com `failureCode` estável
- Referências pendentes são processadas após retry e reprocessamento em background
- Reversões seguem regras específicas de rollback, refund e validação do tipo da referência

## APIs e contratos

### HTTP

A API fornece endpoints para:

- criação de carteira
- consulta de carteira
- ledger e reconciliação
- processamento de apostas
- consulta de transações por provedor ou ID interno
- health checks e métricas

### Autenticação

O serviço valida tokens OIDC e aplica regras de autorização por role:

- `wallet-admin` para acesso interno a carteiras e reconciliação
- `wager-provider` para processamento de transações por provedor

## Execução local

### Requisitos

- Go 1.23+
- Docker + Docker Compose (para cenário completo com PostgreSQL, Keycloak e LocalStack)
- `curl`, `jq` e AWS CLI (opcionais para validação manual)

### Testes locais

Executar a suíte completa:

```sh
export PATH="/home/pc/Imagens/go1.27.1.linux-amd64/go/bin:$PATH"
export CGO_ENABLED=0
cd /home/pc/Documentos/Desafio
go test ./...
```

Executar um pacote específico:

```sh
go test ./internal/app
go test ./internal/domain
```

Execução com race detector (quando o compilador C estiver disponível):

```sh
go test -race ./...
```

## Execução com serviços externos

Para rodar a infraestrutura completa com PostgreSQL, Keycloak e LocalStack:

```sh
docker compose up --build
```

Para testes de integração mais completos, incluindo cenários de concorrência e fila:

```sh
docker compose --profile integration run --rm integration-tests
```

Para executar apenas o teste de concorrência do PostgreSQL:

```sh
docker compose up -d postgres
TEST_DATABASE_URL='postgres://wager:wager-local-only@localhost:5432/wager?sslmode=disable' \
  go test -race ./internal/app -run TestIndependentProcessorsPreserveWalletInvariants -count=1
```

## Health check e observabilidade

Os endpoints principais incluem:

- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

Esses endpoints permitem verificar disponibilidade do serviço e expor métricas de processamento, retries, DLQ, conflitos e outbox.

## Validação

A suíte local do projeto foi validada com sucesso no ambiente atual:

```sh
export PATH="/home/pc/Imagens/go1.27.1.linux-amd64/go/bin:$PATH"
export CGO_ENABLED=0
cd /home/pc/Documentos/Desafio
go test ./...
```

Resultado verificado:

- `ok` para `github.com/desafio/wager-service/cmd/wager-service`
- `ok` para `github.com/desafio/wager-service/internal/app`
- `ok` para `github.com/desafio/wager-service/internal/domain`

## Considerações finais

Este projeto demonstra uma implementação de serviço financeiro com foco em robustez operacional: limites transacionais, invariantes de negócio, processamento de mensagens resiliente e arquitetura pronta para evoluir em um ambiente de produção real.

O código foi estruturado para evidenciar cuidado com consistência, segurança e qualidade de software, aspectos centrais para uma posição técnica em engenharia de backend e sistemas distribuídos.

## Referências

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [migrations/000001_init.up.sql](migrations/000001_init.up.sql)
- [internal/app/processor.go](internal/app/processor.go)
- [internal/domain/wager.go](internal/domain/wager.go)
