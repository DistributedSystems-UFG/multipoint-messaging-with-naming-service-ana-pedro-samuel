[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/YsEblNIV)

# MPComm - Comunicação Multiponto (Edição Banco de Dados Distribuído com Serviço de Nomes)

Este projeto demonstra a evolução de um sistema distribuído. Embora inicialmente fosse uma simples demonstração de comunicação multicast sem coordenação, esta versão implementa um **Banco de Dados Chave-Valor Distribuído** com um protocolo de coordenação centralizado (**Ordem Total via Sequenciador**) e um **Serviço de Nomes** responsável pela descoberta dinâmica dos componentes do sistema.

O objetivo principal é garantir consistência forte entre as réplicas distribuídas e eliminar a necessidade de configuração estática de endereços IP e portas dos participantes.

---

# Estrutura Geral

O sistema é composto pelos seguintes componentes:

* **Naming Service**: responsável pelo registro e descoberta dinâmica de serviços.
* **Comparison Server**: atua como Sequenciador e Coordenador da execução.
* **Peers**: mantêm réplicas locais de um banco de dados chave-valor persistido em arquivos `.txt`.

Cada peer executa o programa `peerCommunicatorUDP.py`, que possui:

* uma thread principal responsável pela submissão de operações ao Sequenciador;
* uma thread `MessageHandler` responsável pelo recebimento e aplicação das operações ordenadas.

O `comparisonServer.py` atua como Sequenciador global. Todas as operações são enviadas a ele através de TCP.

Para cada operação recebida:

1. O servidor atribui um número de sequência global.
2. Realiza broadcast UDP para todos os peers.
3. Os peers armazenam mensagens fora de ordem em buffer.
4. As operações são aplicadas somente quando o número de sequência esperado estiver disponível.

Ao final da execução:

1. O servidor envia um marcador de término (`END`).
2. Cada peer envia seu estado final ao servidor.
3. O servidor compara os bancos de dados finais para verificar consistência.

---

# Arquitetura Atual

```text
                    +--------------------+
                    |   Naming Service   |
                    +--------------------+
                              ^
                              |
            -----------------------------------------
            |                   |                   |
            |                   |                   |
            v                   v                   v

      +-----------+      +-----------+      +-----------+
      |  Peer 1   |      |  Peer 2   |      |  Peer N   |
      +-----------+      +-----------+      +-----------+

                     \        |        /
                      \       |       /
                       \      |      /
                        v     v     v

                  +-------------------+
                  | Comparison Server |
                  +-------------------+
```

Todos os componentes utilizam o Naming Service para localizar serviços e participantes ativos.

---

# Configuração (Setup)

## Infraestrutura

Crie:

* 1 instância para executar o Naming Service;
* 1 instância para executar o Comparison Server;
* N instâncias para executar os peers.

Também é possível executar Naming Service e Comparison Server na mesma máquina.

---

## Configuração

Edite apenas o arquivo `constMP.py`.

O único endereço que deve ser configurado manualmente é o endereço do Naming Service:

```python
SERVICE_NAMES_ADDR = "IP_DO_NAMING_SERVICE"
SERVICE_NAMES_TCP_PORT = 5000
```

Nenhum endereço de peer precisa ser configurado.

Nenhum endereço do Comparison Server precisa ser configurado.

Toda descoberta ocorre dinamicamente.

---

# Componentes do Sistema

## Naming Service

O Naming Service mantém um diretório centralizado contendo os serviços ativos do sistema.

### Operações Disponíveis

#### bind(nome, endereco)

Associa um nome lógico a um endereço.

Exemplo:

```text
bind("peer01", "172.31.10.15:5679")
```

---

#### lookup(nome)

Consulta um registro específico.

Exemplo:

```text
lookup("comparisonServer")
```

---

#### unbind(nome)

Remove um registro existente.

Exemplo:

```text
unbind("peer01")
```

---

#### register(nome, tipo)

Associa um tipo ao registro.

Exemplo:

```text
register("peer01", "peer")
register("comparisonServer", "server")
```

---

#### discover(tipo)

Retorna todos os registros de um determinado tipo.

Exemplo:

```text
discover("peer")
```

Retorno esperado:

```json
[
  {
    "nome": "peer01",
    "endereco": "172.31.10.15:5679"
  },
  {
    "nome": "peer02",
    "endereco": "172.31.10.16:5679"
  }
]
```

---

## ComparisonServer (Sequenciador e Validador)

Responsável por:

* registrar-se no Naming Service;
* descobrir peers ativos;
* iniciar execuções;
* sequenciar operações;
* distribuir operações ordenadas;
* validar o estado final das réplicas.

### Métodos Principais

| Método                               | Descrição                                                      |
| ------------------------------------ | -------------------------------------------------------------- |
| `run()`                              | Loop principal do sistema.                                     |
| `get_peer_list()`                    | Descobre peers através do Naming Service.                      |
| `prepare_peers()`                    | Prepara os peers para uma nova rodada.                         |
| `release_peers()`                    | Libera simultaneamente todos os peers para iniciar a execução. |
| `receive_and_sequence_submissions()` | Recebe operações dos peers e atribui sequência global.         |
| `broadcast_end_marker()`             | Envia marcador de encerramento.                                |
| `collect_final_states()`             | Recebe estados finais dos peers.                               |
| `compare_final_states()`             | Verifica consistência das réplicas.                            |

---

## PeerCommunicator

Responsável por:

* registrar-se automaticamente no Naming Service;
* aguardar autorização do servidor;
* submeter operações;
* receber operações ordenadas;
* manter uma réplica local do banco de dados.

### Métodos Principais

| Método                            | Descrição                              |
| --------------------------------- | -------------------------------------- |
| `run()`                           | Fluxo principal do peer.               |
| `_register_with_naming_service()` | Registra o peer no Naming Service.     |
| `_load_round_configuration()`     | Recebe configuração da execução atual. |
| `send_operations()`               | Gera operações READ e WRITE.           |
| `_submit_operation()`             | Envia operação ao Sequenciador.        |
| `send_final_state_to_server()`    | Envia estado final para validação.     |

---

## MessageHandler

Thread responsável pelo processamento das mensagens ordenadas.

### Métodos Principais

| Método                 | Descrição                             |
| ---------------------- | ------------------------------------- |
| `run()`                | Inicia o recebimento de mensagens.    |
| `_load_or_create_db()` | Cria ou carrega o banco local.        |
| `_receive_messages()`  | Recebe mensagens UDP do Sequenciador. |
| `_process_buffer()`    | Garante aplicação em ordem total.     |
| `_apply_message()`     | Executa READ e WRITE no banco local.  |

---

# Execução do Sistema

## 1. Iniciar o Naming Service

Na máquina responsável pelo Serviço de Nomes:

```bash
python3 namingService.py
```

---

## 2. Iniciar o Comparison Server

Na máquina responsável pelo Sequenciador:

```bash
python3 comparisonServer.py
```

---

## 3. Iniciar os Peers

Em cada máquina peer:

```bash
python3 peerCommunicatorUDP.py
```

---

## 4. Iniciar uma Rodada

No terminal do Comparison Server:

```text
Enter the number of operations for each peer to submit:
```

Informe o número de operações desejado.

O servidor descobrirá automaticamente todos os peers registrados e iniciará a execução.

---

## 5. Encerrar o Sistema

Digite:

```text
0
```

O servidor enviará sinal de parada aos peers registrados e encerrará a execução.

---

# Persistência

Cada peer mantém sua própria réplica local.

Os dados são armazenados em arquivos:

```text
replica_db_peer_X.txt
```

onde `X` representa o identificador do peer.

Os logs de execução são armazenados em:

```text
logfileX.log
```

---

# O que foi modificado a partir da Atividade 2

A principal modificação realizada nesta atividade foi a substituição completa da descoberta estática de processos por um Serviço de Nomes centralizado.

## Remoção da Configuração Estática

Na versão anterior:

* os peers dependiam de endereços previamente conhecidos;
* o servidor dependia do Group Manager para localizar participantes;
* a configuração precisava ser atualizada manualmente.

Na versão atual:

* apenas o endereço do Naming Service permanece configurado;
* todos os demais componentes são descobertos dinamicamente;
* novos peers podem ser adicionados sem alterar arquivos de configuração.

---

## Introdução do Naming Service

Foi criado o componente:

```text
namingService.py
```

Esse componente passou a oferecer as operações:

* `bind(nome, endereco)`
* `lookup(nome)`
* `unbind(nome)`
* `register(nome, tipo)`
* `discover(tipo)`

Essas operações permitem registro, consulta, descoberta e remoção de serviços.

---

## Registro Dinâmico dos Peers

Ao iniciar, cada peer:

1. determina seu endereço local;
2. executa:

```text
bind(nome, endereco)
```

3. registra-se como:

```text
register(nome, "peer")
```

4. torna-se imediatamente disponível para descoberta.

Ao finalizar, executa:

```text
unbind(nome)
```

removendo seu registro do diretório.

---

## Substituição do Group Manager

Na atividade anterior, o componente:

```text
GroupMngr.py
```

era responsável por:

* registrar peers;
* armazenar endereços;
* fornecer listas de participantes.

Com a introdução do Naming Service, essas responsabilidades foram absorvidas pelo diretório de serviços.

Por esse motivo, o Group Manager tornou-se desnecessário para a arquitetura atual.

---

## Descoberta de Peers

O Comparison Server passou a utilizar:

```text
discover("peer")
```

para localizar todos os peers ativos.

Isso permite que participantes sejam adicionados ou removidos dinamicamente sem qualquer alteração de configuração.

---

## Middleware Utilizado

A implementação utiliza:

* sockets TCP para operações do Naming Service;
* sockets TCP para comunicação de controle;
* sockets UDP para distribuição das operações ordenadas;
* serialização utilizando o módulo `pickle`.

Nenhuma dependência externa foi adicionada ao projeto.

---

## Arquivos Alterados

### Novos Arquivos

* `namingService.py`

### Arquivos Modificados

* `comparisonServer.py`
* `peerCommunicatorUDP.py`
* `constMP.py`

### Arquivos Tornados Obsoletos

* `GroupMngr.py`

Suas responsabilidades foram incorporadas ao Naming Service.

---

## Resultado Final

Com a implementação do Serviço de Nomes:

* a configuração estática de peers foi eliminada;
* o sistema passou a suportar descoberta dinâmica;
* o diretório de serviços centralizou o gerenciamento de participantes;
* o Comparison Server localiza automaticamente os peers;
* novos peers podem ingressar no sistema sem reconfiguração manual;
* a arquitetura tornou-se mais flexível e mais próxima de sistemas distribuídos reais.
