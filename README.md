# Lab BigData — Base SIEM & Analyse SOC

## Contexte

Ce document couvre la mise en place d'une stack de streaming distribuée avec Apache Kafka en mode KRaft, accompagnée d'une couche d'observabilité complète (JMX Exporter, Kafka Exporter, Prometheus, Grafana).

Tout l'environnement tourne dans des conteneurs Docker orchestrés par Docker Compose.

### Vue d'ensemble du flux de données

![Flux de données du lab SIEM](assets/diagrams/architecture-flow.svg)

*Schéma global : des sources (réelles ou simulées) jusqu'aux topics Kafka, à la normalisation/corrélation SIEM, à OpenSearch et aux interfaces de visualisation (OpenSearch Dashboards pour l'investigation SOC, Grafana pour le monitoring infra). Les composants en orange pointillé (`24_wazuh_simulator.py`, topic `wazuh-alerts`) sont **simulés** — voir section 19 pour le détail.*

### Roadmap

| Phase | Contenu | Statut |
|---|---|---|
| Phase 0 | Socle Kafka (KRaft) + observabilité (Prometheus/Grafana) | ✅ |
| Phase 1 | Schéma commun + indexation OpenSearch | ✅ |
| Phase 2 | Corrélation cross-sources | ✅ |
| Phase 3 | Source Wazuh — **simulée** (format réel, données générées) | ✅ |
| Phase 4 | Console SOC unifiée (OpenSearch Dashboards) | ✅ |
| Phase 5 | Collecte réelle (agents Wazuh sur VM) + gouvernance | ⬜ à venir |

---

## Structure du projet

```
bigdata-lab_kafka_prometheus_grafana/
├── docker-compose.yml
├── requirements.txt
├── monitoring/
│   ├── jmx/
│   │   ├── jmx_prometheus_javaagent.jar
│   │   └── kafka-jmx.yml
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── kafka-ui/
│   │   └── config.yml
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── datasource.yml
│           └── dashboards/
│               ├── dashboards.yml
│               ├── kafka-overview.json
│               └── soc-overview.json
├── tp/
│   ├── 00_setup.sh
│   ├── 01_producer.py
│   ├── 02_consumer.py
│   ├── 03_processor.py
│   ├── 04_sink_sqlite.py
│   └── 05_query.py
├── log/
│   ├── access.log
│   ├── 09_setup.sh
│   ├── 10_log_producer.py
│   ├── 11_log_processor.py
│   ├── 12_log_query.py
│   ├── 13_live_generator.py
│   └── 14_alert_consumer.py
├── db/
│   ├── 15_setup.sh
│   ├── 16_db_init.py
│   ├── 17_db_simulator.py
│   ├── 18_db_monitor.py
│   └── 19_db_alert_consumer.py
├── siem/
│   ├── 19_setup.sh
│   ├── 20_normalizer.py
│   ├── 21_correlator.py
│   ├── 22_corr_consumer.py
│   ├── 23_setup_wazuh.sh
│   ├── 24_wazuh_simulator.py
│   └── 25_setup_dashboards.sh
└── assets/
    ├── diagrams/
    │   └── architecture-flow.svg
    └── screenshots/
        ├── topic_db-alert.png
        ├── topic_security-alert.png
        ├── topic_corr-alerts.png
        ├── dashboard_vue_d_nesemble.png
        ├── dashboard_surveillance_bdd.png
        ├── dashboard_SOC_.png
        └── openSearch_Dashboard_siem_event.png
```

---

## 1. Apache Kafka en mode KRaft

### 1.1 Qu'est-ce que Kafka ?

Apache Kafka est une plateforme de streaming distribuée. Elle permet à des applications de publier, stocker et consommer des flux de données en temps réel avec une haute performance et une tolérance aux pannes.

Les trois concepts fondamentaux :

- **Producer** : application qui publie des messages dans Kafka
- **Topic** : canal logique dans lequel les messages sont stockés (analogue à une file de messages, mais persistante)
- **Consumer** : application qui lit les messages depuis un topic

### 1.2 KRaft : Kafka sans ZooKeeper

Avant la version 3.x, Kafka dépendait d'Apache ZooKeeper pour gérer les métadonnées du cluster (élection du leader, liste des brokers, état des partitions). Cela impliquait de déployer et maintenir deux systèmes distincts.

Depuis Kafka 4.x, ZooKeeper est supprimé. Le mode **KRaft** (Kafka Raft) intègre la gestion des métadonnées directement dans Kafka via le protocole de consensus Raft.

```
Avant KRaft :                     Avec KRaft :

  ZooKeeper                         Kafka
  (métadonnées)                     (broker + controller)
       |                                  |
     Kafka                           tout en un
     (broker)
```

### 1.3 Les rôles d'un noeud KRaft

Un noeud Kafka peut avoir un ou plusieurs rôles :

| Rôle | Responsabilité |
|---|---|
| `broker` | Reçoit, stocke et sert les messages |
| `controller` | Gère les métadonnées du cluster (remplace ZooKeeper) |

En développement, un seul noeud joue les deux rôles (`broker,controller`). En production, ces rôles sont séparés sur des noeuds distincts pour la résilience.

### 1.4 Les listeners Kafka

Kafka expose plusieurs points d'entrée réseau appelés **listeners**. Chaque listener a un nom, une adresse d'écoute et un protocole de sécurité.

```
CONTROLLER://:9093   --> communication interne KRaft (élection, métadonnées)
INTERNAL://:9092     --> communication entre brokers et services Docker internes
EXTERNAL://:29092    --> accès depuis l'extérieur du réseau Docker (machine hôte)
```

La distinction entre `LISTENERS` et `ADVERTISED_LISTENERS` est importante :

- `KAFKA_LISTENERS` : adresses sur lesquelles Kafka écoute à l'intérieur du conteneur
- `KAFKA_ADVERTISED_LISTENERS` : adresses communiquées aux clients pour qu'ils se reconnectent

```
Client externe (localhost:29092)
         |
    [port mapping Docker]
         |
    EXTERNAL://:29092 (dans le conteneur)
         |
    Kafka répond : "reconnecte-toi sur localhost:29092" (ADVERTISED)

Service Docker interne (kafka-ui, kafka-exporter...)
         |
    INTERNAL://kafka:9092 (réseau Docker)
         |
    Kafka répond : "reconnecte-toi sur kafka:9092" (ADVERTISED)
```

### 1.5 Configuration utilisée

```yaml
KAFKA_NODE_ID: 1
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

KAFKA_LISTENERS: CONTROLLER://:9093,INTERNAL://:9092,EXTERNAL://:29092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

> PLAINTEXT signifie absence de chiffrement et d'authentification. Acceptable en développement, interdit en production.

---

## 2. Kafka UI

Kafka UI (image `provectuslabs/kafka-ui`) est une interface web qui permet d'inspecter le cluster Kafka sans ligne de commande.

Fonctionnalités principales :

- Vue d'ensemble du cluster (brokers, topics, partitions)
- Lecture et publication de messages dans les topics
- Inspection des consumer groups et de leur lag
- Configuration dynamique du cluster

Kafka UI se connecte à Kafka via le listener `INTERNAL` sur le réseau Docker interne :

```yaml
KAFKA_CLUSTER_0_BOOTSTRAPSERVERS: kafka:9092
```

Accessible sur : `http://localhost:8080`

---

## 3. Scripts d'initialisation des topics

Chaque pipeline dispose d'un script shell dédié à la création de ses topics Kafka. Ces scripts servent de **documentation exécutable** : ils sont à la fois la trace écrite des commandes à effectuer et le moyen de les exécuter.

| Script | Topics créés | Usage |
|---|---|---|
| `tp/00_setup.sh` | `events` (3p), `events-stats` (1p) | Pipeline e-commerce |
| `log/09_setup.sh` | `web-logs` (3p), `log-stats` (1p), `security-alerts` (1p) | Pipelines log et sécurité |

Les deux scripts partagent les mêmes principes :

**`--if-not-exists`** — idempotent : sans danger si les topics existent déjà, le script peut être relancé sans erreur.

**`-e KAFKA_OPTS=""`** — neutralise l'agent JMX hérité dans `docker exec` (voir section 15.1 Bugs rencontrés).

**`--replication-factor 1`** — adapté au développement mono-broker. En production avec 3 brokers, passer à `--replication-factor 3`.

Les commandes équivalentes en ligne de commande sont documentées dans la section 22 (Lancer depuis zéro) pour référence.

---

## 4. Volumes Docker : Named Volume vs Bind Mount

Deux mécanismes de persistance sont utilisés dans ce projet, avec des usages distincts.

### Named Volume

```yaml
volumes:
  - kafka-data:/var/lib/kafka/data
```

- Docker gère le stockage dans `/var/lib/docker/volumes/`
- Le contenu n'est pas directement accessible depuis le système de fichiers hôte
- Utilisé pour des données opaques générées par le conteneur (état du broker, séries temporelles Prometheus, configuration Grafana)

### Bind Mount

```yaml
volumes:
  - ./monitoring/jmx:/opt/jmx
```

- Un répertoire réel de la machine hôte est monté dans le conteneur
- Les fichiers sont visibles et modifiables des deux côtés
- Utilisé pour les fichiers de configuration fournis par l'utilisateur

Règle de décision :

| Situation | Type de volume |
|---|---|
| Données générées par le conteneur | Named Volume |
| Fichiers de configuration écrits par l'utilisateur | Bind Mount |
| Fichiers à inspecter ou modifier facilement | Bind Mount |

---

## 5. Couche d'observabilité

### 4.1 Architecture globale

```
Kafka (JVM)
    |
    | KAFKA_OPTS charge l'agent Java au démarrage
    v
JMX Exporter (port 7071)          Kafka Exporter (port 9308)
métriques JVM et broker            métriques applicatives Kafka
    |                                       |
    +-------------------+-------------------+
                        |
                        v
                  Prometheus (port 9090)
                  scrape toutes les 15s
                  stocke les séries temporelles
                        |
                        v
                  Grafana (port 3000)
                  visualise les dashboards
```

### 4.2 JMX Exporter

**JMX** (Java Management Extensions) est le mécanisme standard de la JVM pour exposer des métriques internes : mémoire, threads, garbage collector, et dans le cas de Kafka, des métriques spécifiques au broker.

Le JMX Exporter est un **agent Java** : un jar chargé au démarrage de la JVM via l'option `-javaagent`. Il lit les MBeans JMX et les traduit au format Prometheus.

```
JVM Kafka
  └── MBeans JMX (objets Java internes)
          |
          | JMX Exporter traduit
          v
  /metrics (format texte Prometheus)
  http://kafka:7071/metrics
```

Chargement de l'agent via la variable d'environnement :

```yaml
KAFKA_OPTS: "-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/kafka-jmx.yml"
```

Décomposition de l'argument :

```
-javaagent:        -- option JVM pour charger un agent
/opt/jmx/...jar    -- chemin vers le jar de l'agent
=7071              -- port sur lequel exposer /metrics
:/opt/jmx/...yml   -- fichier de config des règles de transformation
```

Le fichier `kafka-jmx.yml` contient des règles regex qui transforment les noms de MBeans en noms de métriques Prometheus :

```yaml
rules:
  - pattern: "kafka.server<type=(.+), name=(.+)><>Value"
    name: kafka_server_$1_$2
```

Un MBean comme `kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec>` devient la métrique `kafka_server_brokertopicmetrics_messagesinpersec`.

### 4.3 Kafka Exporter

Le Kafka Exporter est un binaire Go (conteneur séparé) qui se connecte au broker Kafka et expose des métriques au niveau applicatif, inaccessibles via JMX :

- `kafka_consumergroup_lag` : retard de consommation par consumer group
- `kafka_topic_partition_current_offset` : offset courant par partition
- `kafka_topic_partitions` : nombre de partitions par topic

Il communique avec Kafka via le listener `INTERNAL` :

```yaml
command:
  - "--kafka.server=kafka:9092"
```

### 4.4 Prometheus

Prometheus est une base de données de séries temporelles. Son mode de fonctionnement est le **scraping** : il interroge périodiquement les endpoints `/metrics` de ses cibles et stocke les valeurs.

```
prometheus.yml définit :
  - la fréquence de scraping (15s)
  - la liste des cibles (targets)

scrape_configs:
  - job_name: 'kafka-jmx'
    targets: ['kafka:7071']           <-- endpoint JMX Exporter

  - job_name: 'kafka-exporter'
    targets: ['kafka-exporter:9308']  <-- endpoint Kafka Exporter
```

L'interface web Prometheus sur `http://localhost:9090/targets` permet de vérifier l'état de chaque cible (UP / DOWN).

### 4.5 Grafana

Grafana est l'outil de visualisation. Il ne stocke pas de métriques — il interroge Prometheus via son API et affiche les résultats sous forme de graphiques.

Le **provisioning** permet de configurer Grafana automatiquement au démarrage du conteneur, sans intervention manuelle dans l'interface. Deux types de provisioning sont utilisés :

**Datasource** (`datasource.yml`) : indique à Grafana où se trouve Prometheus.

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
```

**Dashboard provider** (`dashboards.yml`) : indique à Grafana où chercher les fichiers JSON de dashboards.

```yaml
providers:
  - name: 'kafka-dashboards'
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

Le fichier `kafka-overview.json` est un dashboard sur mesure adapté aux métriques réellement disponibles en Kafka 4.0 KRaft. Il est chargé automatiquement au démarrage dans le dossier `Kafka` de Grafana.

---

## 6. Réseau Docker interne

Docker Compose crée automatiquement un réseau bridge partagé entre tous les services du fichier. Chaque service est accessible par son nom depuis les autres services.

```
Réseau : kafka-bigdata_default

kafka          --> accessible sur kafka:9092 (INTERNAL)
               --> accessible sur kafka:7071 (JMX)
kafka-ui       --> accessible sur kafka-ui:8080
kafka-exporter --> accessible sur kafka-exporter:9308
prometheus     --> accessible sur prometheus:9090
grafana        --> accessible sur grafana:3000
```

C'est pourquoi les configurations internes utilisent des noms de service plutôt que `localhost` :

```yaml
KAFKA_CLUSTER_0_BOOTSTRAPSERVERS: kafka:9092   # pas localhost:9092
url: http://prometheus:9090                     # pas localhost:9090
```

`localhost` dans un conteneur désigne le conteneur lui-même, pas la machine hôte.

---

## 7. Ports exposés sur la machine hôte

| Service | Port hôte | Port conteneur | Usage |
|---|---|---|---|
| Kafka | 29092 | 29092 | Clients externes (producteurs/consommateurs) |
| Kafka | 7071 | 7071 | JMX Exporter /metrics |
| Kafka UI | 8080 | 8080 | Interface web |
| Kafka Exporter | 9308 | 9308 | /metrics applicatifs |
| Prometheus | 9090 | 9090 | Interface web + API |
| Grafana | 3000 | 3000 | Interface web dashboards |

---

## 8. Commandes utiles

### Etat de la stack

```bash
# Voir l'état des conteneurs
docker ps

# Voir les logs d'un service
docker logs kafka
docker logs prometheus
docker logs grafana

# Suivre les logs en temps réel
docker logs -f kafka
```

### Gestion de la stack

```bash
# Démarrer la stack
docker compose up -d

# Arrêter la stack (conserve les volumes)
docker compose down

# Arrêter la stack et supprimer les volumes
docker compose down -v

# Redémarrer un seul service
docker compose restart kafka
```

### Test du broker Kafka

```bash
# Lister les topics
docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Décrire un topic
docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic mon-topic

# Vérifier les offsets d'un topic
docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic mon-topic
```

> Toujours passer `-e KAFKA_OPTS=""` dans docker exec pour neutraliser l'agent JMX
> (voir section 11.1 Bugs rencontrés).

### Vérification des endpoints de métriques

```bash
# Vérifier le JMX Exporter
curl http://localhost:7071/metrics | head -20

# Vérifier le Kafka Exporter
curl http://localhost:9308/metrics | head -20

# Vérifier que Prometheus est prêt
curl http://localhost:9090/-/ready
```

---

## 9. Interfaces web

| Interface | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | aucun |
| Prometheus | http://localhost:9090 | aucun |
| Prometheus Targets | http://localhost:9090/targets | aucun |
| Grafana | http://localhost:3000 | admin / admin |
| OpenSearch Dashboards | http://localhost:5601 | aucun (sécurité désactivée pour le training) |

---

## 10. References

- Apache Kafka KRaft : https://kafka.apache.org/documentation/#kraft
- JMX Exporter : https://github.com/prometheus/jmx_exporter
- Kafka Exporter : https://github.com/danielqsj/kafka_exporter
- Prometheus configuration : https://prometheus.io/docs/prometheus/latest/configuration/configuration/
- Grafana provisioning : https://grafana.com/docs/grafana/latest/administration/provisioning/
- Image Docker Apache Kafka : https://hub.docker.com/r/apache/kafka

---

## 11. Pipeline e-commerce (tp/)

### 10.1 Architecture du pipeline

```
tp/01_producer.py
  simulation e-commerce
  (visiteurs, clics, achats)
        |
        | topic: events (3 partitions)
        |
        v
tp/03_processor.py
  agrégation par fenêtres de 10s
  (total, users uniques, CA, répartition actions)
        |
        | topic: events-stats (1 partition)
        |
        v
tp/04_sink_sqlite.py
  persistance SQLite
        |
        | tp/tp.db
        |
        v
tp/05_query.py
  rapport bilan (lecture seule)

tp/02_consumer.py  <-- lecteur de debug indépendant
  lit directement le topic events
```

### 10.2 Topics Kafka utilisés

| Topic | Partitions | Producteur | Consommateurs |
|---|---|---|---|
| `events` | 3 | `01_producer.py` | `02_consumer.py`, `03_processor.py` |
| `events-stats` | 1 | `03_processor.py` | `04_sink_sqlite.py` |

Les topics sont créés par `00_setup.sh` :

```bash
bash tp/00_setup.sh
```

### 10.3 Environnement Python

Le pipeline Python tourne sur WSL, en dehors des conteneurs Docker. Il se connecte à Kafka via le listener `EXTERNAL` sur `localhost:29092`.

```
WSL (Python)
  producer.py  -->  localhost:29092  -->  [port mapping Docker]  -->  Kafka EXTERNAL
  consumer.py  -->  localhost:29092  -->  [port mapping Docker]  -->  Kafka EXTERNAL
```

Mise en place du venv :

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 10.4 Concepts clés du pipeline

#### Producer (`01_producer.py`)

La clé Kafka est l'identifiant utilisateur (`user-XXX`). Cela garantit que tous les événements d'un même utilisateur vont dans la même partition — propriété utile pour le traitement par session.

```python
producer.send(TOPIC, key=user, value=event)
#                    ^^^^^^^^
#                    clé = user-id -> même partition pour un user donné
```

#### Processor (`03_processor.py`)

Le processor implémente un **fenêtrage temporel** (tumbling window) : il regroupe les événements par tranches de 10 secondes, calcule les agrégats, puis publie le résumé dans `events-stats`.

```
temps  0s      10s      20s      30s
       |--------|--------|--------|
       fenêtre1  fenêtre2  fenêtre3
         flush    flush    flush
```

`auto_offset_reset="latest"` : le processor ne relit pas l'historique — il traite uniquement le flux en temps réel à partir de son démarrage.

#### Sink SQLite (`04_sink_sqlite.py`)

`INSERT OR REPLACE` permet de rejouer des messages sans créer de doublons — propriété d'idempotence importante en streaming.

`auto_offset_reset="earliest"` : le sink relit tout l'historique de `events-stats` au démarrage pour s'assurer qu'aucune fenêtre n'est manquante en base.

---

## 12. Pipeline analyse de logs (log/)

### 11.1 Contexte

Ce pipeline traite des logs d'accès web au format **Apache Combined Log** — le format standard produit par Apache HTTP Server et Nginx. L'objectif est d'ingérer un fichier de log statique dans Kafka, de l'agréger en temps réel et d'en extraire des indicateurs de sécurité et de trafic.

### 11.2 Format Apache Combined Log

Chaque ligne du fichier `log/access.log` suit cette structure :

```
85.50.73.127 - - [09/Jun/2026:08:00:01 +0000] "GET /produits HTTP/1.1" 200 39971 "-" "Mozilla/5.0..."
|            | |  |                         |  |   |                |   |   |      |   |
IP           | |  timestamp                 |  meth path           ver status bytes ref user-agent
             | authuser
             ident
```

Le champ `ident` et `authuser` sont presque toujours `-` dans les configurations modernes.

### 11.3 Architecture du pipeline

```
log/access.log
  5000 lignes Apache Combined Log
        |
        | parsing regex ligne par ligne
        v
log/10_log_producer.py
  clé Kafka = IP source
        |
        | topic: web-logs (3 partitions)
        |
        v
log/11_log_processor.py
  fenêtres de 10s
  agrégats : codes HTTP, top paths, IPs suspectes
        |
        | topic: log-stats (1 partition)
        |
        v
log/12_log_query.py
  rapport terminal
  (trafic, erreurs, top pages, IPs suspectes)
```

### 11.4 Topics Kafka utilisés

| Topic | Partitions | Producteur | Consommateurs |
|---|---|---|---|
| `web-logs` | 3 | `10_log_producer.py` | `11_log_processor.py` |
| `log-stats` | 1 | `11_log_processor.py` | `12_log_query.py` |

### 11.5 Concepts clés du pipeline

#### Parsing regex (`10_log_producer.py`)

Chaque ligne est parsée avec une expression régulière nommée qui extrait les champs utiles :

```python
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+)'           # adresse IP source
    r' \S+ \S+ '             # ident et authuser (ignorés)
    r'\[(?P<ts>[^\]]+)\]'    # timestamp entre crochets
    r' "(?P<method>\S+)'     # méthode HTTP (GET, POST...)
    r' (?P<path>\S+)'        # chemin de la requête
    r' \S+" '                # version HTTP (ignorée)
    r'(?P<status>\d{3})'     # code de statut (200, 404...)
    r' (?P<bytes>\S+)'       # taille de la réponse en octets
    r' "[^"]*"'              # referer (ignoré)
    r' "(?P<ua>[^"]*)"'      # user-agent
)
```

La clé Kafka est l'IP source. Cela garantit que toutes les requêtes d'une même IP atterrissent sur la même partition — propriété utile pour détecter des comportements anormaux par IP.

```python
producer.send(TOPIC, key=event["ip"], value=event)
#                         ^^^^^^^^^
#                         même IP -> même partition
```

#### Agrégation et détection d'anomalies (`11_log_processor.py`)

Pour chaque fenêtre de 10 secondes, le processor calcule :

- nombre total de requêtes
- répartition par classe de code HTTP (`2xx`, `3xx`, `4xx`, `5xx`)
- top 5 des paths les plus demandés
- nombre d'IPs uniques
- volume total de données transféré (bytes)
- liste des IPs suspectes (seuil : > 50 requêtes sur la fenêtre)

```python
# Détection d'IPs suspectes
suspicious = [
    ip for ip, count in bucket["ips"].items()
    if count > 50
]
```

Ce seuil est adapté à un flux en temps réel. Quand le fichier est rejoué en une seule passe (5000 lignes en ~5s), la fenêtre contient beaucoup plus de requêtes par IP qu'en production réelle.

#### Rapport (`12_log_query.py`)

`consumer_timeout_ms=3000` : le consumer s'arrête automatiquement après 3 secondes sans nouveau message — comportement adapté à un script de rapport ponctuel, contrairement aux consumers de pipeline qui tournent en continu.

`enable_auto_commit=False` : le rapport ne commite pas d'offset. Il peut être relancé autant de fois que nécessaire sans affecter les autres consumers.

### 11.6 Résultats observés (run sur 5000 lignes)

```
Requetes totales    : 5000
Volume transfere    : 96.32 MB
IPs suspectes       : 4

Codes HTTP :
  2xx   87.7%   trafic nominal
  3xx    8.4%   redirections (ressources non modifiées, 304...)
  4xx    1.9%   erreurs client (404, 403...)
  5xx    2.0%   erreurs serveur

Methodes :
  GET    93.1%
  POST   13.5%
  PUT     2.2%
  DELETE  1.1%

Top pages : /, /index.html, /produits, /produits/123, /produits/456

IPs suspectes detectees :
  10.0.0.5       (IP privée — service interne ou load balancer)
  192.168.1.10   (IP privée — service interne)
  198.51.100.7   (plage RFC 5737 — réservée aux tests)
  203.0.113.42   (plage RFC 5737 — réservée aux tests)
```

---

## 13. Pipeline détection sécurité temps réel (log/)

### 12.1 Contexte

Ce pipeline étend le pipeline log analysis avec une couche de détection de menaces en temps réel. Il simule ce qu'un **SIEM** (Security Information and Event Management) fait en production : ingérer un flux d'événements, appliquer des règles de corrélation, et publier des alertes classées par sévérité.

Des outils comme Splunk, Elastic SIEM ou Microsoft Sentinel utilisent tous Kafka en entrée de leur pipeline d'ingestion.

### 12.2 Architecture

```
log/13_live_generator.py
  génère un flux HTTP en temps réel
  avec injection automatique de scénarios d'attaque
        |
        | topic: web-logs (3 partitions)
        |
        v
log/11_log_processor.py  (enrichi)
  détecte les anomalies par fenêtres de 10s
        |
        +---> topic: log-stats       (agrégats trafic — inchangé)
        |
        +---> topic: security-alerts (alertes de sécurité)
                        |
                        v
             log/14_alert_consumer.py
             console SOC temps réel
             CRITICAL / HIGH / MEDIUM / LOW
```

### 12.3 Scénarios d'attaque simulés

Le générateur `13_live_generator.py` alterne automatiquement entre les scénarios suivants sur un cycle de ~135 secondes :

| Scénario | Durée | Comportement simulé | Alertes déclenchées |
|---|---|---|---|
| `normal` | 45s | trafic légitime, IPs variées, GET majoritaire | aucune |
| `brute_force` | 15s | 1 IP fixe, rafale sur `/login`, `/admin/` | CRITICAL + HIGH + MEDIUM |
| `scan` | 15s | 1 IP fixe, paths suspects (`/.env`, `/etc/passwd`...) | HIGH + MEDIUM |
| `ddos` | 10s | 3-5 IPs coordonnées, volume x10 | CRITICAL (multiple IPs) |
| `errors` | 10s | burst de réponses 500 sur `/api/checkout` | HIGH |

### 12.4 Règles de détection

Le processor applique 5 règles sur chaque fenêtre de 10 secondes :

| Règle | Sévérité | Condition | Signification |
|---|---|---|---|
| `HIGH_REQUEST_RATE` | CRITICAL | IP > 50 req/fenêtre | brute force ou DDoS |
| `HIGH_5XX_RATE` | HIGH | taux 5xx > 20% | erreurs serveur anormales |
| `SUSPICIOUS_PATH` | HIGH | path dans liste noire | scan de vulnérabilités |
| `MALICIOUS_USER_AGENT` | MEDIUM | UA contient outil offensif | scanner automatisé |
| `HIGH_4XX_RATE` | MEDIUM | taux 4xx > 30% | scan de ressources |

La liste noire de paths couvre les vecteurs d'attaque les plus courants :

```python
SUSPICIOUS_PATHS = {
    "/.env",           # variables d'environnement (secrets, credentials)
    "/etc/passwd",     # fichier système Unix (LFI — Local File Inclusion)
    "/wp-admin/",      # interface admin WordPress
    "/phpmyadmin/",    # interface admin base de données
    "/.git/config",    # exposition du dépôt Git (credentials, historique)
    "/backup.sql",     # dump base de données exposé
    "/config.php",     # fichier de configuration applicative
    "/wp-login.php",   # page de login WordPress (brute force)
}
```

### 12.5 Séparation des topics par domaine

Un principe fondamental de gouvernance des données en streaming est la **séparation des concerns par topic**. Chaque topic a un producteur, des consommateurs et une rétention définis indépendamment.

```
web-logs          logs bruts — rétention courte (24h en prod)
                  consommateurs : processor, outils d'audit

log-stats         agrégats trafic — rétention moyenne (7 jours)
                  consommateurs : Grafana, reporting

security-alerts   alertes de sécurité — rétention longue (90 jours)
                  consommateurs : SOC, SIEM, ticketing
```

Cette séparation permet de donner des droits d'accès différents à chaque topic — un analyste SOC peut lire `security-alerts` sans accès aux logs bruts qui peuvent contenir des données personnelles (IPs = données personnelles au sens du RGPD).

### 12.6 Résultats observés

Pendant le scénario `brute_force` (IP `91.108.4.200`) :

```
[ CRITICAL ] HIGH_REQUEST_RATE
  IP 91.108.4.200 a envoyé 317 requêtes en 10s

[   HIGH   ] SUSPICIOUS_PATH
  Paths suspects : /login, /wp-login.php, /admin/

[  MEDIUM  ] MALICIOUS_USER_AGENT
  User-agents : Nikto/2.1.6, sqlmap/1.7.8, zgrab/0.x, masscan/1.0

[  MEDIUM  ] HIGH_4XX_RATE
  Taux d'erreurs 4xx : 64.0% (203/317 req)
```

Pendant le scénario `ddos` (3 IPs coordonnées) :

```
[ CRITICAL ] HIGH_REQUEST_RATE  IP 193.32.162.10 — 190 req en 10s
[ CRITICAL ] HIGH_REQUEST_RATE  IP 45.33.32.156  — 208 req en 10s
[ CRITICAL ] HIGH_REQUEST_RATE  IP 91.108.4.200  — 203 req en 10s
```

Trois alertes CRITICAL simultanées sur des IPs distinctes = signature caractéristique d'une attaque coordonnée.

### 12.7 Fix Kafka UI — configuration par fichier YAML

**Symptôme** : Kafka UI affiche "No clusters found" malgré les variables `KAFKA_CLUSTER_0_*` dans le `docker-compose.yml`.

**Cause** : les versions récentes de `provectuslabs/kafka-ui` avec `DYNAMIC_CONFIG_ENABLED: "true"` ignorent les variables d'environnement si le fichier de configuration dynamique est absent.

**Solution** : monter un fichier de configuration YAML directement dans le conteneur :

```yaml
# monitoring/kafka-ui/config.yml
kafka:
  clusters:
    - name: lab
      bootstrapServers: kafka:9092
```

```yaml
# docker-compose.yml — service kafka-ui
volumes:
  - ./monitoring/kafka-ui/config.yml:/etc/kafkaui/dynamic_config.yaml
environment:
  DYNAMIC_CONFIG_ENABLED: "true"
```

Le log de démarrage confirme le chargement :

```
INFO: Dynamic config loaded from /etc/kafkaui/dynamic_config.yaml
```

---

## 14. Dashboard Grafana SOC (monitoring/grafana/)

### 13.1 Contexte

Le dashboard `SOC — Détection Sécurité` visualise en temps réel les métriques du pipeline de sécurité directement depuis le Kafka Exporter via Prometheus. Il ne nécessite aucun code supplémentaire — il exploite les métriques déjà exposées par l'infrastructure existante.

Accessible sur : `http://localhost:3000` > Dashboards > Kafka > SOC — Détection Sécurité

### 13.2 Panels et métriques

Le dashboard contient 8 panels organisés en 3 lignes :

**Ligne 1 — Stats instantanées**

| Panel | Métrique Prometheus | Signification |
|---|---|---|
| Alertes publiées (total) | `sum(kafka_topic_partition_current_offset{topic="security-alerts"})` | Nombre total d'alertes générées depuis le démarrage |
| Lag SOC analyst | `sum(kafka_consumergroup_lag{topic="security-alerts", consumergroup="soc-analyst"})` | Alertes en attente de lecture — lag élevé = SOC débordé |
| Messages web-logs (total) | `sum(kafka_topic_partition_current_offset{topic="web-logs"})` | Volume total de requêtes HTTP ingérées |
| Lag processor | `sum(kafka_consumergroup_lag{topic="web-logs", consumergroup="log-processor"})` | Messages web-logs non encore traités |

**Ligne 2 — Séries temporelles trafic**

| Panel | Métrique | Ce qu'on observe pendant une attaque |
|---|---|---|
| Débit entrant web-logs | `rate(kafka_topic_partition_current_offset{topic="web-logs"}[1m])` | Pic brutal sur une partition = IP suspecte (clé = IP) |
| Alertes publiées (cumul) | `kafka_topic_partition_current_offset{topic="security-alerts"}` | Montée en marches = fenêtres d'attaque successives |

**Ligne 3 — Séries temporelles lag et offset**

| Panel | Métrique | Ce qu'on observe pendant une attaque |
|---|---|---|
| Lag log-processor | `kafka_consumergroup_lag{topic="web-logs", consumergroup="log-processor"}` | Pic absorbé puis retour à 0 = consumer sain |
| Offset par partition | `kafka_topic_partition_current_offset{topic="web-logs"}` | Partition qui accélère seule = IP très active |

### 13.3 Lecture des signaux d'attaque

La clé Kafka du topic `web-logs` étant l'IP source, toutes les requêtes d'une même IP atterrissent sur la même partition. Ce choix de conception rend les attaques directement visibles dans Grafana :

```
Trafic normal :
  partition-0  ████░░░░░░  trafic équilibré
  partition-1  ████░░░░░░
  partition-2  ████░░░░░░

Brute force (1 IP) :
  partition-0  ████░░░░░░
  partition-1  ████░░░░░░
  partition-2  ████████████████████  <- pic = IP attaquante

DDoS (3 IPs coordonnées) :
  partition-0  ████████████  <- IP 1
  partition-1  ████████████  <- IP 2
  partition-2  ████████████  <- IP 3
```

### 13.4 Seuils de couleur

Les stats utilisent des seuils de couleur pour attirer l'attention :

| Panel | Vert | Orange | Rouge |
|---|---|---|---|
| Alertes publiées | 0-9 | 10-49 | 50+ |
| Lag SOC analyst | 0-4 | 5-19 | 20+ |
| Lag processor | 0-99 | 100-499 | 500+ |

### 13.5 Résultats observés

Capture réalisée pendant un scénario `ddos` :

```
Alertes publiées  : 72  (orange — seuil 10 dépassé)
Lag SOC analyst   : 5   (orange — consumer légèrement en retard)
Messages web-logs : 11.9K
Lag processor     : 9   (vert — processor absorbe le surplus)

Débit web-logs    : pic à 15 req/s sur partition-2 (bleu)
                    partitions 0 et 1 stables
Alertes cumul     : courbe plate puis montée en marches à 15:55
Lag processor     : pic à 12.5 sur partition-2 puis retour à 0
Offset partitions : partition-1 et partition-2 accélèrent ensemble
                    -> 2 IPs coordonnées = signature DDoS
```

---


## 15. Pipeline surveillance base de données (db/)

### Architecture

PostgreSQL (sensitive_db)
        |
        | requêtes SQL simulées
        v
db/17_db_simulator.py  -->  topic: db-queries  -->  db/18_db_monitor.py
                                                           |
                                                           | topic: db-alerts
                                                           v
                                                     db/19_db_alert_consumer.py

### Base de données cible

PostgreSQL 16 avec 4 tables de données sensibles :

| Table | Contenu | Classification |
|---|---|---|
| clients | nom, email, téléphone, adresse, date de naissance | RGPD critique |
| paiements | numéro de carte, IBAN, montant | PCI-DSS critique |
| contrats | clauses confidentielles, valeur | Confidentiel |
| employes | salaire, numéro de sécurité sociale | RH sensible |

### Utilisateurs simulés

| User | Profil | DDL autorisé |
|---|---|---|
| app_user | application web, requêtes ciblées | non |
| analyst | analyste métier, agrégats | non |
| dba_user | DBA, requêtes système | oui |
| intern_user | stagiaire, accès limité | non |
| attacker | compte compromis (IP 185.220.101.45) | non |

### Scénarios d'exfiltration simulés

| Scénario | Durée | Comportement | Indicateur |
|---|---|---|---|
| normal | 40s | trafic légitime varié | baseline |
| dump_table | 15s | SELECT * sans WHERE sur tables sensibles | volumétrie + absence filtre |
| recon | 15s | requêtes sur information_schema, pg_user | accès métadonnées système |
| mass_exfil | 20s | rafale de SELECT avec grands LIMIT | volume + répétition |
| priv_escalation | 10s | CREATE USER, GRANT, COPY depuis compte non-admin | DDL non autorisé |
| scraping | 20s | SELECT paginés sur clients/paiements | pattern itératif |

### Règles de détection (db/18_db_monitor.py)

| Sévérité | Règle | Condition |
|---|---|---|
| CRITICAL | TABLE_DUMP | SELECT * sans WHERE sur table sensible |
| CRITICAL | UNAUTHORIZED_DDL | DDL depuis compte non-admin |
| HIGH | HIGH_QUERY_VOLUME | > 30 requêtes / fenêtre 10s par user |
| HIGH | DB_RECON | accès à information_schema, pg_tables, pg_user |
| HIGH | HIGH_ROWS_RETURNED | > 500 lignes retournées / fenêtre 10s par user |
| MEDIUM | MULTI_TABLE_ACCESS | accès à 3+ tables sensibles sur une fenêtre |

### Concepts clés

Clé Kafka = user_db — toutes les requêtes d'un même utilisateur vont sur la même partition. Un pic sur une partition = un utilisateur anormalement actif, visible directement dans Grafana et Kafka UI.

Faux positif intentionnel — analyst déclenche régulièrement MULTI_TABLE_ACCESS car il accède naturellement à plusieurs tables. En production, on ajouterait une whitelist par profil utilisateur.

DDL simulé sans exécution — les requêtes CREATE USER, GRANT, COPY sont simulées sans exécution réelle pour ne pas corrompre la base. Le monitor les détecte via le champ query_type de l'événement Kafka.

### Résultats observés

Sur un cycle complet de scénarios (environ 3 minutes) :
- 268 CRITICAL — principalement TABLE_DUMP pendant dump_table et mass_exfil
- 72 HIGH — DB_RECON en rafale pendant recon, HIGH_ROWS_RETURNED jusqu'à 32 375 lignes/10s
- 16 MEDIUM — MULTI_TABLE_ACCESS sur utilisateurs légitimes et attaquant

### Lancer le pipeline

    bash db/15_setup.sh
    python db/16_db_init.py
    python db/19_db_alert_consumer.py  # Terminal 1
    python db/18_db_monitor.py          # Terminal 2
    python db/17_db_simulator.py        # Terminal 3

---

## 16. Dashboard Grafana DLP (monitoring/grafana/)

### Accès

http://localhost:3000 -> Dashboards -> DLP — Surveillance Base de Données

### Panels

| Panel | Métrique | Lecture |
|---|---|---|
| Alertes DLP total | sum offset db-alerts | monte par paliers lors des attaques |
| Lag analyst DLP | lag db-alerts group db-analyst | doit rester proche de 0 |
| Requêtes SQL total | sum offset db-queries | croissance continue |
| Lag monitor DB | lag db-queries group db-monitor | pic temporaire pendant mass_exfil |
| Débit SQL par partition | rate offset db-queries 1m | pic sur 1 partition = user suspect |
| Alertes DLP cumul | offset db-alerts | marches = scénarios d'attaque |
| Lag db-monitor partition | lag db-queries db-monitor | absorption des bursts |
| Offset partition db-queries | offset db-queries | partition attaquant décroche |

### Lecture des signaux d'attaque

Trafic normal — les 3 partitions de db-queries progressent à vitesse similaire.

dump_table / mass_exfil — la partition de l'attaquant décroche brutalement des autres. Le panel Alertes DLP cumul monte en pente raide simultanément.

recon — pas de pic de débit, mais le panel Alertes DLP cumul monte par petites marches régulières — signature d'une reconnaissance méthodique.

priv_escalation — débit faible mais alertes CRITICAL UNAUTHORIZED_DDL immédiates dans db-alerts.

---


## 17. Pipeline SIEM — Normalisation + Indexation OpenSearch (siem/)

### Contexte

Les pipelines existants (web, sécurité, BDD) produisent chacun leur propre format JSON. Un analyste ne peut pas faire de recherche cross-sources. La Phase 1 SIEM résout ce problème en ajoutant une couche de normalisation et un stockage indexé commun.

### Architecture

    security-alerts  ──┐
    db-alerts        ──┤──> siem/20_normalizer.py ──> OpenSearch siem-events-YYYY.MM.DD
    web-logs         ──┘

### OpenSearch

OpenSearch 2.13.0 est ajouté au docker-compose.yml (port 9200, 512 Mo RAM, mode single-node sans TLS pour le training). Les données sont persistées dans un volume nommé opensearch-data.

### Schéma commun (10 champs)

| Champ | Type | Description |
|---|---|---|
| @timestamp | datetime | Horodatage ISO 8601 |
| source_type | keyword | web / security / db |
| severity | keyword | CRITICAL / HIGH / MEDIUM / LOW / INFO |
| rule | keyword | Règle déclenchée |
| description | text | Description lisible |
| ip_source | ip | Adresse IP concernée |
| user | keyword | Utilisateur concerné |
| host | keyword | Hôte concerné |
| tags | keyword[] | Labels libres |
| raw | object | Event original complet |

### siem/20_normalizer.py

Consomme les 3 topics simultanément, mappe chaque format source vers le schéma commun, indexe dans un index partitionné par jour (siem-events-YYYY.MM.DD). Les requêtes HTTP normales (2xx/3xx) sont filtrées — seules les anomalies sont indexées.

### Comment OpenSearch reçoit les données

OpenSearch n'accède jamais à Kafka directement — il n'a aucune notion des topics. Le pont entre les deux mondes est `siem/20_normalizer.py`, qui agit comme **consumer Kafka** d'un côté et **client REST OpenSearch** de l'autre :

```
Kafka topics                    siem/20_normalizer.py              OpenSearch
(security-alerts,                                                  (port 9200)
 db-alerts,           ──>  consumer Kafka  ──>  POST /siem-events-YYYY.MM.DD/_doc
 web-logs,                 (port 29092)         via opensearch-py
 wazuh-alerts)                                  (API REST)
```

Dans le code, ce pont tient en deux lignes :

```python
os_client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
...
os_client.index(index=current_index, body=doc)
```

`opensearch-py` n'est qu'un wrapper autour de l'API REST d'OpenSearch : `os_client.index(...)` envoie un `POST http://localhost:9200/siem-events-2026.06.11/_doc` avec le document JSON normalisé. Pour OpenSearch, ce document arrive comme n'importe quel appel HTTP — il ne sait pas qu'il provient de Kafka.

**Conséquence importante** : le normalizer doit tourner en continu pour que les Dashboards affichent des données à jour. S'il est arrêté, Kafka continue d'accumuler les messages (visibles via le lag du consumer group `siem-normalizer` dans Grafana), mais OpenSearch n'indexe plus rien tant qu'il n'est pas relancé.

En production, ce rôle de pont serait tenu par Logstash ou un connecteur Kafka Connect (sink OpenSearch) — le même principe (consumer Kafka + écriture via API REST), mais avec gestion native des retries, du backpressure et du dead-letter queue.

### Concepts clés

Index par jour — même pattern qu'en production dans les stacks Wazuh ou Elastic SIEM. Permet de définir des politiques de rétention par date (chaud/tiède/froid) sans modifier le code.

Filtrage à l'indexation — indexer toutes les requêtes HTTP ferait exploser le volume. On n'indexe que les status >= 400, ce qui réduit le volume d'un facteur ~8 tout en conservant les signaux utiles.

### Résultats observés

Sur un cycle de génération active : 646 documents indexés en quelques minutes, répartis entre source_type=web (erreurs HTTP) et source_type=security (alertes détectées). L'agrégation par sévérité confirme la distribution attendue : MEDIUM majoritaire, CRITICAL rare.

### Lancer

    docker compose up -d opensearch
    python siem/20_normalizer.py   # à lancer en parallèle des pipelines existants

---

## 18. Pipeline SIEM — Corrélation cross-sources (siem/)

### Contexte

La normalisation (Phase 1) permet de voir tous les events au même endroit. La corrélation (Phase 2) exploite cette vue unifiée pour détecter des enchaînements d'événements qui, pris isolément, sont suspects mais pas critiques — et qui ensemble constituent une chaîne d'attaque confirmée.

### Architecture

    OpenSearch siem-events-*
           |
           | requête toutes les 60s
           v
    siem/21_correlator.py ──> topic: corr-alerts ──> siem/22_corr_consumer.py

Le correlator lit OpenSearch (pas Kafka directement) pour pouvoir raisonner sur l'historique des dernières minutes et croiser des sources différentes.

### Règles de corrélation

| Règle | Sources | Fenêtre | Condition | Sévérité |
|---|---|---|---|---|
| RECON_TO_EXFIL | security + db | 5 min | scan web SUSPICIOUS_PATH + dump BDD TABLE_DUMP coexistent | CRITICAL |
| MULTI_SOURCE_CRITICAL | security + db | 2 min | alertes CRITICAL simultanées sur les deux sources | CRITICAL |
| SUSTAINED_ATTACK | security | 3 min | même IP déclenche 3+ règles distinctes HIGH/CRITICAL | HIGH |

### Résultat observé en live

MULTI_SOURCE_CRITICAL s'est déclenchée lors du premier test : HIGH_REQUEST_RATE (brute force web sur 193.32.162.10) + TABLE_DUMP (dump BDD par attacker) dans la même fenêtre de 2 minutes. Chaîne de preuves affichée dans la console investigation avec timestamps, source, sévérité et règle pour chaque event.

### Concepts clés

Corrélation temporelle — deux events isolés deviennent un incident quand ils coexistent dans une fenêtre de temps. C'est le principe fondamental des SIEM comme Splunk ou Elastic Security.

Preuve vs alerte — le consumer 22_corr_consumer.py n'affiche pas juste une alerte : il affiche la chaîne des events qui l'ont déclenchée. C'est ce qu'un analyste SOC voit lors d'une investigation, pas juste un message d'erreur.

Faux positif maîtrisé — MULTI_SOURCE_CRITICAL exige deux sources distinctes. Un CRITICAL isolé (brute force seul, ou dump BDD seul) ne suffit pas. La corrélation réduit le bruit par définition.

### Lancer

    bash siem/19_setup.sh                  # créer le topic corr-alerts
    python siem/22_corr_consumer.py        # Terminal 1 — console investigation
    python siem/21_correlator.py           # Terminal 2 — moteur de corrélation
    python siem/20_normalizer.py           # Terminal 3 — normalisation
    python log/11_log_processor.py         # Terminal 4 — détection web
    python db/18_db_monitor.py             # Terminal 5 — détection BDD
    python db/17_db_simulator.py           # Terminal 6 — simulation BDD
    python log/13_live_generator.py        # Terminal 7 — génération trafic

---

## 19. Pipeline SIEM — Simulation Wazuh (siem/)

### Contexte

Les phases 1 et 2 normalisent et corrèlent les sources déjà présentes dans le lab (web, sécurité, BDD). En production SOC réelle, une source majeure manque : un HIDS (Host Intrusion Detection System) comme Wazuh, qui surveille directement les machines (authentification, intégrité de fichiers, processus). La Phase 3 ajoute cette source.

Aucune VM n'étant disponible en environnement de training, un vrai agent Wazuh ne peut pas être déployé. La phase simule donc le flux d'alertes qu'un Wazuh Server publierait dans Kafka, au format exact attendu — de sorte que normalizer et correlator traitent ces events exactement comme s'ils venaient d'un vrai déploiement.

### Architecture

    siem/24_wazuh_simulator.py ──> topic: wazuh-alerts ──> siem/20_normalizer.py ──> OpenSearch siem-events-YYYY.MM.DD
                                                                    |
                                                                    v
                                                          siem/21_correlator.py (règles WAZUH_*)

### Principe d'isolation par topic

Le simulateur publie dans `wazuh-alerts` exactement comme le ferait un Wazuh Server connecté à de vraies VMs (agents `srv-web-01`, `srv-db-01`, `srv-admin-01`, `workstation-01`). Le jour où ces VMs existeront, seul le producteur change : un vrai Wazuh Server remplace `24_wazuh_simulator.py`. Le normalizer (`20_normalizer.py`) et le correlator (`21_correlator.py`) restent intacts, car ils consomment le topic `wazuh-alerts` sans connaître son origine.

### Format Wazuh

Un event Wazuh suit cette structure (reproduite fidèlement par le simulateur) :

| Champ | Exemple | Description |
|---|---|---|
| timestamp | 2026-06-11T10:42:01.000+0000 | Horodatage ISO 8601 |
| rule.id | 5710 | Identifiant de la règle Wazuh déclenchée |
| rule.level | 5 | Niveau de sévérité Wazuh (1-15) |
| rule.groups | ["syslog", "sshd", "authentication_failed"] | Catégories de la règle |
| agent.name | srv-web-01 | Nom de l'hôte surveillé (clé Kafka) |
| agent.ip | 10.0.1.10 | IP de l'hôte surveillé |
| data.srcip | 185.220.101.45 | IP source de l'événement (attaquant ou utilisateur) |

### Agents et scénarios simulés

| Agent | Rôle | IP |
|---|---|---|
| srv-web-01 | serveur web frontend | 10.0.1.10 |
| srv-db-01 | serveur base de données | 10.0.1.20 |
| srv-admin-01 | serveur d'administration | 10.0.1.5 |
| workstation-01 | poste développeur | 10.0.1.100 |

| Scénario | Description |
|---|---|
| normal | activité système légitime (échecs PAM occasionnels, FIM bénin) |
| ssh_bruteforce | rafale d'échecs SSH depuis une IP externe sur srv-web-01 |
| fim_alert | modification de fichier système critique (/etc/passwd, sshd_config...) |
| privilege_esc | élévation de privilèges suspecte (sudo vers root, création d'utilisateur) |
| lateral_move | la même IP externe réussit une connexion SSH sur plusieurs hôtes |
| webshell | exécution de commande suspecte via une requête HTTP |

Le simulateur s'appuie sur 13 règles Wazuh réelles (SSH, PAM, sudo, FIM, attaques web, rootkit), avec leurs id, level et groups officiels.

### Mapping severity (normalizer)

`from_wazuh_alerts()` dans `20_normalizer.py` traduit `rule.level` (échelle Wazuh 1-15) vers le champ `severity` du schéma commun :

| rule.level | severity |
|---|---|
| 1-3 | INFO |
| 4-6 | MEDIUM |
| 7-9 | HIGH |
| 10-15 | CRITICAL |

### Nouvelles règles de corrélation

| Règle | Source | Fenêtre | Condition | Sévérité |
|---|---|---|---|---|
| WAZUH_LATERAL_MOVE | wazuh (WAZUH_5720) | 5 min | la même IP source réussit une connexion SSH sur 2+ hôtes distincts | CRITICAL |
| WAZUH_FIM_CRITICAL | wazuh (WAZUH_550 / WAZUH_553) | 5 min | modification de fichier système sur un ou plusieurs hôtes de production | HIGH |

### Bug rencontré : champ host manquant dans query_events

`query_events()` dans `21_correlator.py` restreint les champs récupérés depuis OpenSearch via `_source`. La liste initiale ne contenait pas `host`, alors que les deux nouvelles règles regroupent les events par hôte (`e.get("host", "")`). Résultat : `host` valait toujours `""`, les events n'étaient jamais regroupés par hôte, et WAZUH_LATERAL_MOVE et WAZUH_FIM_CRITICAL ne se déclenchaient jamais malgré des données conformes dans OpenSearch.

**Solution** : ajouter `"host"` à la liste `_source` de `query_events()`.

```python
"_source": ["@timestamp", "source_type", "severity",
            "rule", "description", "ip_source", "user", "host"],
```

### Résultats observés en live

WAZUH_LATERAL_MOVE (CRITICAL) : l'IP 185.220.101.45 (nœud de sortie Tor) a réussi une connexion SSH sur 4 hôtes distincts en moins de 5 minutes — mouvement latéral confirmé.

WAZUH_FIM_CRITICAL (HIGH) : modifications de fichiers système (/etc/passwd, sshd_config...) détectées simultanément sur 4 hôtes — indicateur de persistance ou de compromission généralisée.

### Ce qui est simulé vs ce qu'apporterait un vrai Wazuh

Cette section précise honnêtement la portée de la Phase 3 : le pipeline est validé de bout en bout, mais la **source** de données est artificielle.

**Ce qu'on a simulé :**

```
24_wazuh_simulator.py  →  wazuh-alerts  →  normalizer  →  OpenSearch
```

**Ce qu'un vrai déploiement Wazuh apporterait :**

```
Wazuh Agent (sur VM)
    │ collecte réelle
    │   /var/log/auth.log, /var/log/syslog
    │   processus système, intégrité fichiers (FIM)
    ▼
Wazuh Manager
    │ corrélation native + règles MITRE ATT&CK (3000+ règles)
    ▼
Wazuh Indexer (OpenSearch embarqué)
    ▼
Wazuh Dashboard
```

Les briques manquantes :

- **Un vrai agent sur une vraie machine** — `24_wazuh_simulator.py` n'observe rien, il invente des events. En production, un `ssh_bruteforce` Wazuh provient de vraies tentatives de connexion dans `/var/log/auth.log`.
- **Le Wazuh Manager** — le vrai moteur de corrélation embarque 3000+ règles natives, la détection de rootkits et le mapping MITRE ATT&CK. Notre `21_correlator.py` a 5 règles artisanales (dont 2 spécifiques Wazuh).
- **Le FIM (File Integrity Monitoring) réel** — le simulateur génère des events `fim_modified` aléatoires. Un vrai agent calcule un checksum SHA-256 sur chaque fichier surveillé et détecte une modification effective.

**Pourquoi ça ne se voit pas dans OpenSearch Dashboards** : dans Discover, tous les events `source_type: wazuh` proviennent de `24_wazuh_simulator.py`. Rien ne les distingue visuellement d'un vrai agent — le format est strictement identique. C'est précisément le piège pédagogique : le pipeline fonctionne, mais la source est creuse.

**Conséquence pour la roadmap** : la Phase 3 est une **validation d'intégration**, pas une collecte réelle. Elle prouve que le format Wazuh est correctement géré par le normalizer et le correlator. Le jour où de vraies VMs seront disponibles (Phase 5), le seul changement sera de remplacer `24_wazuh_simulator.py` par un vrai Wazuh Manager qui publie dans `wazuh-alerts` — tout le reste (topic, normalizer, correlator, Dashboards) reste intact.

### Lancer

    bash siem/23_setup_wazuh.sh            # créer le topic wazuh-alerts
    python siem/24_wazuh_simulator.py      # en plus des 6 terminaux existants

---

## 20. Pipeline SIEM — Console SOC OpenSearch Dashboards (siem/)

### Contexte

Les phases 1 à 3 normalisent (`20_normalizer.py`), corrèlent (`21_correlator.py`) et enrichissent (`24_wazuh_simulator.py`) les events dans OpenSearch. Mais l'accès aux données reste limité à des requêtes `curl` sur l'API OpenSearch ou à la console `22_corr_consumer.py`. Un analyste SOC a besoin d'une interface de recherche et d'investigation visuelle.

Grafana et OpenSearch Dashboards répondent à deux besoins différents et complémentaires :

| Outil | Usage | Ce qu'on voit |
|---|---|---|
| Grafana | Monitoring infra | Métriques Kafka, lag, débit, offsets — via Prometheus |
| OpenSearch Dashboards | Investigation SOC | Events individuels, recherche full-text, pivot, timeline — via OpenSearch |

Grafana répond à « est-ce que le pipeline tourne normalement ? ». OpenSearch Dashboards répond à « que s'est-il passé sur cette IP / cet hôte / cette plage horaire ? ». La Phase 4 ajoute ce second outil.

### docker-compose.yml

Le service `opensearch-dashboards:2.13.0` est ajouté, exposé sur le port 5601, avec `DISABLE_SECURITY_DASHBOARDS_PLUGIN: "true"` (pas de TLS/authentification pour le training) et une dépendance sur le service `opensearch`.

### siem/25_setup_dashboards.sh

Configure automatiquement OpenSearch Dashboards au démarrage :

1. Attend que le service soit prêt (retry 10s x12)
2. Crée l'index pattern `siem-events-*` avec `@timestamp` comme champ temporel
3. Le définit comme index par défaut
4. Crée l'index pattern `corr-alerts` (optionnel)

### Deux API REST distinctes

Ce script ne touche pas OpenSearch directement — il appelle l'**API REST de Dashboards** (port 5601), pas celle d'OpenSearch (port 9200) :

```bash
curl -X POST "http://localhost:5601/api/saved_objects/index-pattern/siem-events" ...
```

C'est de la **configuration** de Dashboards ("voici les index que tu dois savoir lire et avec quel champ temporel"), pas de l'écriture de données. Les deux flux REST sont donc complètement séparés :

| Composant | API appelée | Port | Rôle |
|---|---|---|---|
| `siem/20_normalizer.py` | API REST OpenSearch | 9200 | Écrit les documents (`POST /siem-events-*/_doc`) |
| `siem/25_setup_dashboards.sh` | API REST Dashboards | 5601 | Configure les index patterns (saved objects) |
| `opensearch-dashboards` (conteneur) | API REST OpenSearch | 9200 (réseau Docker interne) | Lit les documents pour les afficher dans Discover |

Dashboards ne stocke aucune donnée lui-même : il interroge OpenSearch en interne via `OPENSEARCH_HOSTS: '["http://opensearch:9200"]'` (voir docker-compose.yml) à chaque requête dans Discover.

### Lancer

```bash
docker compose up -d opensearch-dashboards
bash siem/25_setup_dashboards.sh
```

### Discover : événements SIEM normalisés en temps réel

![OpenSearch Dashboards Discover — events Wazuh normalisés](assets/screenshots/openSearch_Dashboard_siem_event.png)

*Discover affiche les events Wazuh en temps réel avec tous les champs normalisés visibles : `source_type`, `severity`, `rule`, `host`, `ip_source`, `tags`, `raw.*`.*

### 3 usages SOC validés dans Discover

| Usage | Requête | Objectif |
|---|---|---|
| Filtrer par source | `source_type : wazuh` | Isoler les events provenant d'une source donnée |
| Investiguer une IP suspecte cross-sources | `ip_source : 185.220.101.45` | Retrouver toute l'activité d'une IP, tous topics confondus |
| Filtrer par sévérité | `severity : CRITICAL` | Se concentrer sur les incidents les plus graves |

### Vue Kafka UI des topics SIEM

![Topic db-alerts dans Kafka UI](assets/screenshots/topic_db-alert.png)

*Le topic `db-alerts` contient les alertes DLP déclenchées par les règles `TABLE_DUMP` et `MULTI_TABLE_ACCESS`.*

![Topic security-alerts dans Kafka UI](assets/screenshots/topic_security-alert.png)

*Le topic `security-alerts` contient les alertes `HIGH_REQUEST_RATE`, `SUSPICIOUS_PATH` et `MALICIOUS_USER_AGENT`.*

![Topic corr-alerts dans Kafka UI](assets/screenshots/topic_corr-alerts.png)

*Le topic `corr-alerts` contient l'alerte `MULTI_SOURCE_CRITICAL`, avec sa chaîne de preuves embarquée.*

### Dashboards Grafana (monitoring infra)

![Grafana — Kafka vue d'ensemble](assets/screenshots/dashboard_vue_d_nesemble.png)

*Le dashboard Kafka — Vue d'ensemble montre les 9 topics actifs, le lag des consumers et les offsets par topic.*

![Grafana — surveillance base de données](assets/screenshots/dashboard_surveillance_bdd.png)

*Le dashboard DLP montre le pic de débit SQL lors d'un `dump_table`, avec le lag absorbé par le consumer.*

![Grafana — dashboard SOC](assets/screenshots/dashboard_SOC_.png)

*Le dashboard SOC montre les pics de débit `web-logs` lors des scénarios d'attaque, avec les alertes en marches d'escalier.*

---

## 21. Bugs rencontrés et leçons apprises

### 15.1 KAFKA_OPTS hérité dans docker exec

**Symptôme** : toute commande `docker exec kafka kafka-topics.sh ...` échoue avec :

```
FATAL ERROR in native method: processing of -javaagent failed
java.net.BindException: Address in use
```

**Cause** : la variable `KAFKA_OPTS` est définie dans l'environnement du conteneur pour charger l'agent JMX sur le port 7071. Quand `docker exec` lance un nouveau processus Java dans le conteneur, il hérite de cette variable et tente de démarrer un second agent JMX sur le même port — déjà occupé par le broker.

**Solution** : neutraliser `KAFKA_OPTS` pour toutes les commandes `docker exec` :

```bash
docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh ...
#           ^^^^^^^^^^^^^^^^
#           écrase la variable pour ce processus uniquement
```

Le script `00_setup.sh` applique systématiquement cette pratique.

---

### 15.2 Topic __consumer_offsets non créé automatiquement

**Symptôme** : le consumer Python se connecte mais ne reçoit aucun message. Les logs Kafka montrent une boucle infinie :

```
Sent auto-creation request for Set(__consumer_offsets) to the active controller.
Sent auto-creation request for Set(__consumer_offsets) to the active controller.
...
```

**Cause** : `__consumer_offsets` est le topic interne dans lequel Kafka stocke les positions de lecture (offsets) de tous les consumer groups. Après un `docker compose down -v`, ce topic doit être recréé — mais sur certaines configurations KRaft en mode combiné `broker,controller`, le broker se retrouve en deadlock avec lui-même pendant cette création.

**Solution immédiate** : créer le topic manuellement :

```bash
docker exec -e KAFKA_OPTS="" kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic __consumer_offsets \
  --partitions 50 \
  --replication-factor 1 \
  --config cleanup.policy=compact
```

**Solution pérenne** : ajouter ces variables dans le service `kafka` du `docker-compose.yml` :

```yaml
KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: 50
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

---

### 15.3 Clé Kafka vs champ du payload

**Symptôme** : `KeyError: 'user_id'` dans le consumer et le processor.

**Cause** : l'identifiant est passé comme **clé Kafka** du message, pas comme champ du payload JSON. Dans le consumer, `msg.value` donne le payload JSON. La clé est dans `msg.key`.

**Solution** :

```python
# Lire la clé depuis msg.key, pas depuis msg.value
user = msg.key.decode("utf-8") if msg.key else "unknown"
```

---

### 15.4 Bug d'indentation dans le sink

**Symptôme** : le sink tourne sans erreur mais n'écrit rien en base.

**Cause** : une erreur d'indentation plaçait le `commit()` en dehors de la boucle `for msg in consumer`. En Python, l'indentation définit la structure logique — elle n'est pas cosmétique.

```python
# INCORRECT : commit hors de la boucle
for msg in consumer:
    conn.execute("INSERT ...")
conn.commit()       # exécuté une seule fois, après arrêt du consumer

# CORRECT : commit dans la boucle
for msg in consumer:
    conn.execute("INSERT ...")
    conn.commit()   # exécuté à chaque message
```

---

### 15.5 Noms de métriques JMX changés en Kafka 4.0

**Symptôme** : certains panels Grafana affichent "No data" malgré des targets Prometheus UP.

**Cause** : les dashboards communautaires sont écrits pour des versions antérieures de Kafka. En Kafka 4.0 KRaft, certains noms de métriques JMX ont changé.

| Ancienne métrique (Kafka 2.x/3.x) | Nouvelle métrique (Kafka 4.x) |
|---|---|
| `kafka_controller_kafkacontroller_activecontrollercount` | `kafka_server_metadataloader_currentcontrollerid` |
| `kafka_server_brokertopicmetrics_messagesin_total` | non disponible via JMX dans cette config |

**Solution** : explorer les métriques disponibles via `http://localhost:9090/graph` avec le préfixe `kafka_`, puis adapter les requêtes du dashboard JSON.

---

### 15.6 Variable de datasource non résolue dans le dashboard JSON

**Symptôme** : Grafana affiche "Datasource ${DS_PROMETHEUS_WH211} was not found".

**Cause** : le dashboard JSON contient une variable de datasource destinée à être résolue lors d'un import manuel via l'interface. En provisioning automatique via fichier, cette résolution n'a pas lieu.

**Solution** :

```bash
# Remplacer la variable par le nom de la datasource
sed -i 's/\${DS_PROMETHEUS_WH211}/Prometheus/g' \
  monitoring/grafana/provisioning/dashboards/kafka-overview.json

# Vérifier l'uid réel via l'API Grafana si nécessaire
curl -s http://admin:admin@localhost:3000/api/datasources \
  | python3 -m json.tool | grep -E '"uid"|"name"'
```

---

## 22. Lancer le projet depuis zéro

### Stack Docker

```bash
# Démarrer tous les conteneurs
docker compose up -d

# Vérifier que tous les conteneurs sont UP
docker ps
```

### Pipeline e-commerce

```bash
# Créer les topics via le script
# tp/00_setup.sh crée events (3 partitions) et events-stats (1 partition)
# Il utilise --if-not-exists : sans danger si les topics existent déjà
bash tp/00_setup.sh

# Activer le venv
source .venv/bin/activate

# Lancer le pipeline (3 terminaux séparés)
python tp/01_producer.py    # terminal 1
python tp/03_processor.py   # terminal 2
python tp/04_sink_sqlite.py # terminal 3

# Consulter le bilan
python tp/05_query.py
```

### Pipeline analyse de logs

```bash
# Créer les topics via le script (équivalent aux commandes manuelles ci-dessous)
bash log/09_setup.sh

# Ou manuellement :
# docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
#   --bootstrap-server localhost:9092 --create --if-not-exists \
#   --topic web-logs --partitions 3 --replication-factor 1
#
# docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
#   --bootstrap-server localhost:9092 --create --if-not-exists \
#   --topic log-stats --partitions 1 --replication-factor 1

# Activer le venv
source .venv/bin/activate

# Lancer le pipeline (2 terminaux séparés)
python log/11_log_processor.py  # terminal 1 (démarrer en premier)
python log/10_log_producer.py   # terminal 2

# Consulter le rapport
python log/12_log_query.py
```

### Pipeline détection sécurité temps réel

```bash
# Créer les topics via le script (crée les 3 topics d'un coup)
bash log/09_setup.sh

# Ou manuellement pour le seul topic security-alerts :
# docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
#   --bootstrap-server localhost:9092 --create --if-not-exists \
#   --topic security-alerts --partitions 1 --replication-factor 1

# Activer le venv
source .venv/bin/activate

# Lancer le pipeline (3 terminaux séparés)
python log/14_alert_consumer.py   # terminal 1 (démarrer en premier)
python log/11_log_processor.py    # terminal 2
python log/13_live_generator.py   # terminal 3

# Observer les alertes en temps réel dans le terminal 1
# Les scénarios d'attaque se déclenchent automatiquement toutes les ~15-45s
```

### Pipeline SIEM complet (siem/)

Prérequis : stack Docker démarrée avec OpenSearch, et pipelines log + db opérationnels.

```bash
# Démarrer OpenSearch et OpenSearch Dashboards si pas encore fait
docker compose up -d opensearch opensearch-dashboards

# Configurer la console SOC (index patterns)
bash siem/25_setup_dashboards.sh

# Créer le topic corr-alerts
bash siem/19_setup.sh

# Activer le venv
source .venv/bin/activate

# Créer le topic wazuh-alerts
bash siem/23_setup_wazuh.sh

# Lancer dans 8 terminaux (ordre important)
python siem/22_corr_consumer.py   # T1 — console investigation
python siem/21_correlator.py      # T2 — corrélation cross-sources
python siem/20_normalizer.py      # T3 — normalisation -> OpenSearch
python log/11_log_processor.py    # T4 — détection web
python db/18_db_monitor.py        # T5 — détection BDD
python db/17_db_simulator.py      # T6 — simulation BDD
python log/13_live_generator.py   # T7 — génération trafic
python siem/24_wazuh_simulator.py # T8 — simulation Wazuh (4 agents)

# Vérifier le contenu OpenSearch
curl -s http://localhost:9200/siem-events-$(date +%Y.%m.%d)/_count
```

### Interfaces web

| Interface | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | aucun |
| Prometheus Targets | http://localhost:9090/targets | aucun |
| Grafana | http://localhost:3000 | admin / admin |
| Dashboard Kafka | http://localhost:3000/dashboards | Dashboards > Kafka > Kafka — Vue d'ensemble |
| Dashboard SOC | http://localhost:3000/dashboards | Dashboards > Kafka > SOC — Détection Sécurité |
| Dashboard DLP | http://localhost:3000/dashboards | Dashboards > Kafka > DLP — Surveillance Base de Données |
| OpenSearch API | http://localhost:9200 | aucun (désactivé pour training) |
| OpenSearch index | http://localhost:9200/siem-events-*/_count | comptage documents indexés |
| OpenSearch Dashboards | http://localhost:5601 | aucun (sécurité désactivée pour le training) |