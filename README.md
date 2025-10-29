# Querys

Repositório voltado para documentar as querys de extração de dados utilizados no monitoramento.

| Query | Descrição|
|---|---|
| [Disponibilidade](/disponibilidade/disp.sql) | Calcula a disponibilidade diária por instituição e API |
| [P95](/desempenho/p95_new.sql) | Calcula o percentil 95 do tempo de resposta por API |
| [Reports](/reports/pairment.sql) | Calcula a taxa de report diária por instituição |
| [Sobrecarga do servidor](/sobrecarga/529.sql) | Calcula a taxa de requisições 529 sobre as requisições válidas por instituição |
| [Volumes](/volumes/vol.sql) | Extrai os volumes das instituições como ```CLIENT``` e como ```SERVER``` por API |
| [Chamadas Válidas](/chamadas_validas/valid.sql) | Calcula o volume de requisições válidas de acordo com a regra de disponibilidade |