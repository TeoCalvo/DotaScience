# Dota Science

<img src="https://i.ibb.co/KbmHPsP/dota-Science-banner-01.jpg" alt="" width="550">

Este projeto nasceu a partir da vontade de nossa comunidade ([Téo Me Why](https://www.twitch.tv/teomewhy)) em unir a área de dados e games. Assim, estamos desenvolvendo uma maneira de coletar, armazenar, organizar e analisar dados de partidas de Dota2.

A API que consultamos para ingestões de dados é a [www.opendota.com](https://www.opendota.com/), em que os _end-points_ são disponibilizados de forma gratuita com uma limitação mensal de requisições.

**Importante**:
Todos comandos executados apresentados neste arquivo são executados a partir da pasta raiz do projeto.

Sinta-se livre para contribuir, compartilhar e divulgar este material de forma gratuíta, mas ressaltamos a proibição da comercialização deste material, sob a licença Creative Commons BY-NC-SA 3.0 BR.

<img src="https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-sa.png" alt="" width="200">

## Requisitos

- Docker & Docker Compose
- Python
- Anaconda

## Instalação

```bash
git clone https://github.com/TeoCalvo/DotaScience.git <nome_da_pasta>
```

```bash
cd <nome_da_pasta>
```

## Uso

### Preparação do ambiente

0. Subindo nosso banco de dados com docker
```bash
docker-compose up -d
```

1. Criando novo ambiente Python

```sh
conda create --name dota-env python=3.
```

2. Ativando ambiente python

```sh
conda activate dota-env
```

3. Instalando dependências

```sh
pip install -r requirements.txt
```

### Obtendo lista das partidas históricas (profissionais)

Ao rodar pela primeira vez, use o argumento '```--how newest```' , coletando assim as partidas mais recentes.

```bash
python dotaScience/hook/get_match_history.py --how newest
```

Caso o processo seja interrompidoo, é necessário dar inicio a partir da última partida coletada:

```bash
python dotaScience/hook/get_match_history.py --how oldest
```

### Obtendo detalhes das partidas coletadas

```bash
python dotaScience/hook/get_match_details.py
```

### Migrando dados do MongoDB para MariaDB

```bash
python dotaScience/magic_wand/mongo_to_maria.py
```
