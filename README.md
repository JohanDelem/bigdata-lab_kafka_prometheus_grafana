# Lab BigData — Stack Kafka + Observabilité

## Contexte

Ce document couvre la mise en place d'une stack de streaming distribuée avec Apache Kafka en mode KRaft, accompagnée d'une couche d'observabilité complète (JMX Exporter, Kafka Exporter, Prometheus, Grafana).

Tout l'environnement tourne dans des conteneurs Docker orchestrés par Docker Compose.

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
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── datasource.yml
│           └── dashboards/
│               ├── dashboards.yml
│               └── kafka-overview.json
├── tp/
│   ├── 00_setup.sh
│   ├── 01_producer.py
│   ├── 02_consumer.py
│   ├── 03_processor.py
│   ├── 04_sink_sqlite.py
│   └── 05_query.py
├── log/
│   ├── access.log
│   ├── 10_log_producer.py
│   ├── 11_log_processor.py
│   ├── 12_log_query.py
│   ├── 13_live_generator.py
│   └── 14_alert_consumer.py
└── monitoring/
    ├── ...
    └── kafka-ui/
        └── config.yml
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

## 3. Volumes Docker : Named Volume vs Bind Mount

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

## 4. Couche d'observabilité

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

## 5. Réseau Docker interne

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

## 6. Ports exposés sur la machine hôte

| Service | Port hôte | Port conteneur | Usage |
|---|---|---|---|
| Kafka | 29092 | 29092 | Clients externes (producteurs/consommateurs) |
| Kafka | 7071 | 7071 | JMX Exporter /metrics |
| Kafka UI | 8080 | 8080 | Interface web |
| Kafka Exporter | 9308 | 9308 | /metrics applicatifs |
| Prometheus | 9090 | 9090 | Interface web + API |
| Grafana | 3000 | 3000 | Interface web dashboards |

---

## 7. Commandes utiles

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

## 8. Interfaces web

| Interface | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | aucun |
| Prometheus | http://localhost:9090 | aucun |
| Prometheus Targets | http://localhost:9090/targets | aucun |
| Grafana | http://localhost:3000 | admin / admin |

---

## 9. References

- Apache Kafka KRaft : https://kafka.apache.org/documentation/#kraft
- JMX Exporter : https://github.com/prometheus/jmx_exporter
- Kafka Exporter : https://github.com/danielqsj/kafka_exporter
- Prometheus configuration : https://prometheus.io/docs/prometheus/latest/configuration/configuration/
- Grafana provisioning : https://grafana.com/docs/grafana/latest/administration/provisioning/
- Image Docker Apache Kafka : https://hub.docker.com/r/apache/kafka

---

## 10. Pipeline e-commerce (tp/)

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

## 11. Pipeline analyse de logs (log/)

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

## 12. Pipeline détection sécurité temps réel (log/)

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

## 13. Bugs rencontrés et leçons apprises

### 13.1 KAFKA_OPTS hérité dans docker exec

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

### 13.2 Topic __consumer_offsets non créé automatiquement

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

### 13.3 Clé Kafka vs champ du payload

**Symptôme** : `KeyError: 'user_id'` dans le consumer et le processor.

**Cause** : l'identifiant est passé comme **clé Kafka** du message, pas comme champ du payload JSON. Dans le consumer, `msg.value` donne le payload JSON. La clé est dans `msg.key`.

**Solution** :

```python
# Lire la clé depuis msg.key, pas depuis msg.value
user = msg.key.decode("utf-8") if msg.key else "unknown"
```

---

### 13.4 Bug d'indentation dans le sink

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

### 13.5 Noms de métriques JMX changés en Kafka 4.0

**Symptôme** : certains panels Grafana affichent "No data" malgré des targets Prometheus UP.

**Cause** : les dashboards communautaires sont écrits pour des versions antérieures de Kafka. En Kafka 4.0 KRaft, certains noms de métriques JMX ont changé.

| Ancienne métrique (Kafka 2.x/3.x) | Nouvelle métrique (Kafka 4.x) |
|---|---|
| `kafka_controller_kafkacontroller_activecontrollercount` | `kafka_server_metadataloader_currentcontrollerid` |
| `kafka_server_brokertopicmetrics_messagesin_total` | non disponible via JMX dans cette config |

**Solution** : explorer les métriques disponibles via `http://localhost:9090/graph` avec le préfixe `kafka_`, puis adapter les requêtes du dashboard JSON.

---

### 13.6 Variable de datasource non résolue dans le dashboard JSON

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

## 14. Lancer le projet depuis zéro

### Stack Docker

```bash
# Démarrer tous les conteneurs
docker compose up -d

# Vérifier que tous les conteneurs sont UP
docker ps
```

### Pipeline e-commerce

```bash
# Créer les topics
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
# Créer les topics (si absents)
docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic web-logs --partitions 3 --replication-factor 1

docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic log-stats --partitions 1 --replication-factor 1

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
# Créer le topic security-alerts (si absent)
docker exec -e KAFKA_OPTS="" kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic security-alerts --partitions 1 --replication-factor 1

# Activer le venv
source .venv/bin/activate

# Lancer le pipeline (3 terminaux séparés)
python log/14_alert_consumer.py   # terminal 1 (démarrer en premier)
python log/11_log_processor.py    # terminal 2
python log/13_live_generator.py   # terminal 3

# Observer les alertes en temps réel dans le terminal 1
# Les scénarios d'attaque se déclenchent automatiquement toutes les ~15-45s
```

### Interfaces web

| Interface | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | aucun |
| Prometheus Targets | http://localhost:9090/targets | aucun |
| Grafana | http://localhost:3000 | admin / admin |