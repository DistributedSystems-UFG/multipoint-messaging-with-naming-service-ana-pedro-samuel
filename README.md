[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/YsEblNIV)

# MPComm - Comunicação Multiponto (Edição Banco de Dados Distribuído)
Este projeto demonstra a evolução de um sistema distribuído. Embora inicialmente fosse uma simples demonstração de comunicação multicast sem coordenação, esta versão implementa um **Banco de Dados Chave-Valor Distribuído** com um protocolo de coordenação centralizado (Ordem Total via um Sequenciador) para garantir consistência forte entre as réplicas. Ele demonstra como resolver problemas de ordenação de mensagens à medida que um sistema distribuído escala na dimensão geográfica.

## Estrutura Geral
Um conjunto de processos pares (*peers*) é estabelecido, cada um mantendo uma réplica local de um banco de dados (persistido em arquivos `.txt`). Em vez de enviar mensagens em multicast diretamente uns para os outros e contar com a sorte, os peers agora submetem operações (`READ` ou `WRITE`) a um **Servidor de Comparação** central que atua como um **Sequenciador**.

O Sequenciador atribui um número de sequência global a cada operação recebida e transmite (via broadcast UDP) a operação sequenciada para todos os peers. Cada peer recebe essas operações e utiliza um buffer local para garantir que elas sejam aplicadas ao seu banco de dados estritamente na sequência definida pelo servidor.

Os processos pares executam o programa `peerCommunicatorUDP.py`, que possui duas threads separadas: a thread principal para submeter operações (via TCP para o servidor) e uma thread `MessageHandler` para receber os broadcasts ordenados (via UDP do servidor), armazená-los em buffer, se necessário, e aplicá-los ao DB local.

Um processo separado executa o `comparisonServer.py`. Ele atua como Sequenciador e Coordenador. Ele sinaliza o início para os peers, sequencia todas as requisições `submit` recebidas, faz o broadcast delas e, por fim, coleta o estado final (`final_state`) de cada peer. Em seguida, compara o conteúdo final dos bancos de dados para garantir que todas as réplicas atingiram exatamente o mesmo estado (Execução em Ordem Total).

Um segundo processo de servidor, executando o `GroupMngr.py`, é usado para que os peers registrem a si mesmos (notavelmente seus endereços IP) via TCP, bem como para descobrir os outros peers na rede.

Ao executar isso em um ambiente de nuvem distribuído (por exemplo, distribuído em várias regiões geográficas), você pode observar como o mecanismo de buffer lida com entregas UDP fora de ordem introduzidas pela latência da rede, garantindo que as réplicas do banco de dados permaneçam perfeitamente sincronizadas.

## Configuração (Setup)
- Crie uma instância (máquina virtual na nuvem) para executar os dois servidores e um número de instâncias adicionais (ex.: 6 em uma região da nuvem e 2 em outra) para executar os peers.
- Edite o arquivo `constMP.py` com o endereço IP correto da instância do servidor. **Dica:** aloque um endereço IP fixo (Elastic IP, no jargão da AWS) para a instância do servidor.
- Na primeira instância, execute o `GroupMngr.py`
- Nas outras instâncias (peers): execute o `peerCommunicatorUDP.py`
- De volta à primeira instância: execute o `comparisonServer.py`

## Classes Principais

### `ComparisonServer` (Sequenciador e Validador)
| Método | Descrição |
|---|---|
| `run()` | Loop principal de coordenação e controle de inputs. |
| `get_peer_list()` | Obtém lista de peers registrados do GroupManager. |
| `start_peers()` | Envia sinal TCP de início a cada peer com o número de operações. |
| `receive_and_sequence_submissions()`| Recebe operações dos peers, atribui um número de sequência global (`seq`) e faz o broadcast (UDP). Também coleta os estados finais. |
| `broadcast_end_marker()` | Envia um marcador `END` em multicast para sinalizar o fim do stream. |
| `compare_final_states()` | Compara os dicionários do banco de dados de cada peer para garantir a consistência das réplicas. |

### `GroupManager`
| Método | Descrição |
|---|---|
| `run()` | Loop de aceite de conexões TCP. |
| `_dispatch()` | Roteia o tipo de requisição recebida (`register`, `list`, `unregister`, `stop`). |
| `_handle_register()` | Registra o IP e porta de um novo peer. |
| `_handle_list()` | Envia a lista atualizada de IPs ao solicitante. |
| `_handle_unregister()` | Remove um peer da lista de ativos. |

### `PeerCommunicator`
| Método | Descrição |
|---|---|
| `run()` | Loop principal do peer (aguarda início, envia handshakes, submete requisições). |
| `register_with_group_manager()` | Obtém o IP público via API e registra no GroupManager. |
| `send_operations()` | Gera operações de `READ` ou `WRITE` aleatórias. |
| `_submit_operation()` | Envia a operação desejada ao Sequenciador (ComparisonServer) via TCP. |
| `send_final_state_to_server()` | Envia o snapshot final do Banco de Dados local e os logs ao servidor para validação. |

### `MessageHandler` (Thread)
| Método | Descrição |
|---|---|
| `run()` | Inicia a espera por handshakes e o loop de recebimento de mensagens. |
| `_load_or_create_db()` | Cria uma base de dados local (`.txt`) com 100 registros iniciais ou carrega uma existente. |
| `_receive_messages()` | Recebe as mensagens sequenciadas via UDP do servidor. |
| `_process_buffer()` | Garante a ordenação total: enfileira pacotes UDP fora de ordem e aplica no DB apenas quando o `next_expected_seq` correto chega. |
| `_apply_message()` | Executa operações de leitura (`READ`) ou escrita (`WRITE`) no banco de dados e gera logs locais. |

## Execução do Sistema

Siga os passos abaixo para executar o sistema em um ambiente distribuído:

1. Crie uma instância (uma máquina virtual na nuvem) para executar os dois servidores, e um conjunto de instâncias adicionais para executar os peers.
   *(Exemplo: 6 em uma região da nuvem e 2 em outra).*

2. Edite o arquivo `constMP.py` com o endereço IP correto da instância do servidor.  
   **Dica:** utilize um IP fixo (Elastic IP, na AWS) para evitar mudanças de endereço e problemas de conexão.

3. Na primeira instância, execute: `python3 GroupMngr.py`

4. Nas outras instâncias (peers), execute: `python3 peerCommunicatorUDP.py`

5. De volta à primeira instância, execute: `python3 comparisonServer.py`
   *(O servidor pedirá o número de operações que cada peer deve realizar. Após a inserção, o teste começará automaticamente)*
   *(Recomendo inicialmente dar uma carga de 1 operação apenas para criar os arquivos base na máquina, após isso, podem ser realizados um número maior de operações)*
