# Problema 2 — Produtor/Consumidor

## Implementação e invariantes

Duas estratégias compartilham configuração, carga e coleta de métricas. Cada
produtor gera IDs exclusivos `idProdutor*n + índice`. O total esperado é `p*n`.
Contagens e listas são exclusivas de cada goroutine e lidas depois dos WaitGroups.
O amostrador é aguardado antes de ler suas métricas. A verificação final rejeita
IDs fora do intervalo, duplicações, quantidade incorreta e buffer não vazio.
Isso é mais forte que apenas comparar contagens, pois uma duplicação poderia
compensar a perda de outro item.

Na versão channel, a capacidade K fornece a limitação e sincronização. O
coordenador aguarda todos os produtores antes de fechar o canal; consumidores
continuam até drenar os itens. Assim, não há envio em canal fechado.

Na versão semáforos, `notFull` inicia com K permissões e `notEmpty` com zero.
Produzir segue: adquirir notFull → mutex → inserir no índice tail → liberar
mutex → sinalizar notEmpty. Consumir segue: adquirir notEmpty → mutex → retirar
no índice head → liberar mutex → sinalizar notFull. Os índices avançam módulo K.
O mutex protege buffer, índices e ocupação, que permanece entre zero e K.
Durante operações em andamento há permissões reservadas: não se deve exigir que
os comprimentos dos dois canais somem K em todo instante.

Depois de aguardar os produtores, o coordenador insere uma sentinela por
consumidor. Como não há mais produção e o buffer é FIFO, nenhum item de trabalho
fica atrás das sentinelas. Cada consumidor retira no máximo uma sentinela e sai.
O coordenador aguarda todos, incluindo aqueles ainda processando itens anteriores.
Sentinelas não contam como itens produzidos/consumidos, mas ocupam o buffer.

## Deadlock

Nenhuma goroutine espera uma permissão de semáforo segurando o mutex. Isso permite
que o outro lado altere o buffer e sinalize a permissão necessária. Quando o
buffer está cheio, consumidores podem esvaziá-lo; quando vazio e ainda há produção,
produtores podem preenchê-lo. Com produtores e consumidores positivos, trabalho
finito e goroutines eventualmente escalonadas, os protocolos permitem progresso.
Não há ciclo de espera imposto pelo algoritmo. A sentinela também respeita K,
inclusive K=1; consumidores continuam ativos enquanto o coordenador as insere.

No channel, o fechamento depois do WaitGroup elimina a espera eterna por itens
após a produção. Um consumidor sair ao primeiro timeout quebraria o protocolo:
produtores poderiam ficar bloqueados sem quem drenasse o buffer. Por isso o
consumidor 0 usa `select` + `time.After` apenas para registrar inatividade e tentar
novamente; timeout não prova que a produção terminou.

## Starvation e distribuição

Ausência de deadlock não implica justiça. Não há garantia de divisão uniforme
entre consumidores nem de limite máximo de espera pelo scheduler/mutex/canais.
Consumidores podem receber quantidades muito diferentes, inclusive zero em uma
execução curta. Compare `per_consumer` em várias repetições e cargas. Uma execução
em que todos consumiram não prova ausência de starvation em geral. O término
pressupõe que goroutines prontas eventualmente executem; não há garantia formal
de espera limitada nesta implementação. O consumidor 0 também paga o custo dos
timers, tornando a distribuição assimétrica.

## Efeito de K e metodologia

K=1 força coordenação frequente e pouco desacoplamento. K=10 e K=100 absorvem
rajadas maiores e podem reduzir bloqueios. Aumentar K não garante maior
throughput: o gargalo pode ser processamento, mutex, alocações, timers ou scheduler.
Quando consumidores são lentos, a ocupação tende a subir; quando produtores são
lentos, tende a cair. Memória do buffer cresce com K e filas maiores podem aumentar
o tempo que um item espera; esta implementação não mede latência por item.

O throughput considera itens reais consumidos divididos pelo tempo da simulação,
incluindo inicialização e encerramento. A ocupação é uma média de amostras com
intervalo solicitado de 100us, e não uma média temporal exata. Atrasos do scheduler,
poucas amostras e sentinelas podem enviesá-la. A instrumentação também interfere
na execução (mutex do amostrador, IDs armazenados e time.After). Portanto, estes
resultados caracterizam o programa instrumentado, não o custo isolado da primitiva.

Execute os comandos de experimentos do README, mantenha P, C, N, atrasos, versão do Go e ambiente
constantes e compare repetições (média e dispersão). Separe cargas sem atraso,
produção lenta e consumo lento. Use `-race` para validação; suas medições de tempo
não devem ser misturadas às de execuções normais. A aprovação do detector cobre
os entrelaçamentos executados, não é prova matemática de ausência de races.

## Uso de IA

Esta integração foi gerada com assistência do Codex a partir dos requisitos do
usuário. A IA inspecionou a estrutura, preservou a implementação de filósofos e
copiou o rascunho anterior para `rascunhos/main-original.go.txt`; implementou as
duas estratégias, testes, script e documentação. O histórico disponibilizado não
incluía o código completo anterior, então não se afirma reprodução literal dele.
O autor do trabalho deve revisar, compreender e explicar o código e complementar
este registro com as regras da disciplina e com suas próprias alterações.

## Experimento executado nesta integração

Execução em 07/10/2026, Go 1.27.1, darwin/amd64, sem `-race`.
P=2, C=3, N=5000 por produtor, sem atrasos; três repetições por combinação.
Dados brutos: `resultados-exemplo.jsonl`. Todas as 18 execuções verificaram
10.000 itens produzidos e consumidos, sem duplicações.

| Versão | K | Throughput médio (itens/s) | Mín.–máx. (itens/s) | Ocupação média amostrada |
|---|---:|---:|---:|---:|
| channel | 1 | 1596360 | 1542401–1624084 | 0.44 |
| channel | 10 | 2648452 | 2596894–2729495 | 4.25 |
| channel | 100 | 4185759 | 4055816–4302228 | 39.19 |
| semaphore | 1 | 829441 | 774489–856963 | 0.40 |
| semaphore | 10 | 1781580 | 1728078–1862720 | 4.97 |
| semaphore | 100 | 2619743 | 2531319–2672688 | 36.92 |

Estas execuções curtas são uma demonstração reproduzível da coleta, não um
benchmark conclusivo. A dispersão e a baixa quantidade de amostras limitam a
interpretação da ocupação. Repita com cargas mais longas e atrasos controlados
antes de concluir que uma estratégia ou capacidade é superior.

Validação: `go test -race ./... -count=1 -timeout=60s`, `go vet ./...` e
`go run -race` nas duas versões com K=1. A parte de filósofos permaneceu
inalterada; o teste do módulo compila esse pacote, mas não exercita seu protocolo.
