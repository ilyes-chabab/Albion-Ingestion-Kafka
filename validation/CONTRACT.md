# Contrat de données : prix du marché Albion Online

| | |
|---|---|
| **Version** | 1.0.0 (`x-contract-version` dans le schéma, `validator_version` dans la DLQ) |
| **Producteur** | Connecteur Hugging Face, `connectors/albion_connector.py`, via `kafka/scripts/producer.py` |
| **Topic d'entrée** | `albion.market.prices` (brut) |
| **Topics de sortie** | `albion.market.prices.validated` (valides) et `albion.market.prices.dlq` (rejetés) |
| **Schéma** | [`schemas/market_price.schema.json`](schemas/market_price.schema.json), JSON Schema 2020-12 |
| **Règles supplémentaires** | [`rules.py`](rules.py), dans la fonction `business_errors` |
| **Tests** | [`test_rules.py`](test_rules.py) |

Le schéma décrit la **forme** des données. Ce contrat ajoute les engagements sur le **sens** des données, leur **qualité**, leur **fraîcheur** et la façon dont elles **évoluent**.

## 1. Sémantique : ce que veut dire chaque champ

Un message représente **l'état du marché pour un objet, dans une ville, à une qualité donnée**, au moment de la collecte par l'Albion Online Data Project (serveur européen).

| Champ | Signification | Unité / format |
|---|---|---|
| `source` | Origine du message | texte, par exemple `HF_albion_online_market_prices` |
| `dataset_sha` | Version du dataset Hugging Face d'où vient la ligne | hash git de 40 caractères |
| `extract_date` | Moment où le connecteur a lu le fichier | ISO 8601, UTC |
| `data.item_id` | Identifiant interne de l'objet dans le jeu (`T4_BAG`, `ADEPT'S+TORCH`…) | texte en majuscules |
| `data.city` | Marché où le prix est observé, parmi les 8 villes dont le Black Market | texte (liste fermée) |
| `data.quality` | Qualité de l'objet : 1 normal, 2 bon, 3 remarquable, 4 excellent, 5 chef-d'œuvre | entier |
| `data.sell_price_min` / `max` | Prix de vente le plus bas / le plus haut proposé par les vendeurs | argent du jeu (silver), entier |
| `data.buy_price_min` / `max` | Prix d'achat le plus bas / le plus haut proposé par les acheteurs | argent du jeu (silver), entier |
| `data.*_date` | Moment où le prix correspondant a été observé | ISO 8601, heure UTC sans fuseau |

**Convention de la source** : un prix à `0` avec une date `null` signifie **« aucun ordre »**, et non « gratuit ». À la normalisation, ce prix devient `null`.

## 2. Qualité : ce qui rend un message invalide

| Règle | Où elle est vérifiée |
|---|---|
| Champs obligatoires présents, aucun champ inconnu (`additionalProperties: false`) | schéma |
| Types respectés : texte, entier, nombre, null | schéma |
| `quality` de 1 à 5 ; `city` dans la liste ; `item_id` conforme au motif | schéma |
| Prix ≥ 0 et entiers | schéma (`minimum`, `multipleOf`) |
| Dates au format ISO 8601 et réellement possibles (pas de mois 13) | schéma + `rules.py` |
| Un prix > 0 a une date ; un prix à 0 n'en a pas | `rules.py` |
| `*_price_min ≤ *_price_max` lorsque les deux existent | `rules.py` |

Toutes ces règles sont respectées à 100 % par le fichier réel (47 926 lignes). Un rejet signale donc une **vraie anomalie**.

**Stratégie en cas de violation**, d'après les trois options vues en cours :
- **Quarantaine** (par défaut) : le message est isolé dans la DLQ avec son erreur, et le traitement continue.
- **Alerte** : un WARNING est émis si plus de 5 % des messages d'une exécution sont rejetés.
- **Arrêt immédiat** (optionnel) : avec `--fail-on-alert`, le script se termine avec le code 2, et la tâche Airflow est marquée en échec.

## 3. Fraîcheur

- **Cohérence temporelle** (règle bloquante) : une date d'observation ne peut pas être postérieure à `extract_date`.
- **Fraîcheur du lot** (indicateur, non bloquant) : le bilan du validateur donne `latest_observation`, la date d'observation la plus récente. C'est elle qui indique l'âge réel des données.
- **Engagement de la source** : le dataset est un snapshot publié ponctuellement, sans fréquence garantie. Le pipeline vérifie chaque jour s'il a changé (`sha`). Une alerte de fraîcheur, par exemple « aucune nouvelle version depuis N jours », relève de l'étape 6 (observabilité).

## 4. Unicité et doublons

- **Clé métier** : `item_id|city|quality`. C'est la clé Kafka et la clé de partition.
- La même clé peut revenir plusieurs fois avec des prix ou des dates différents. Ce sont des **observations successives**, pas des doublons. Le fichier source en contient 280.
- **Doublon** : deux messages dont le contenu métier normalisé est identique, c'est-à-dire avec la même `fingerprint` (SHA-256). Les métadonnées d'extraction n'entrent pas dans le calcul.

## 5. Gestion des évolutions

Le contrat suit un **versionnement sémantique** : `MAJEUR.MINEUR.CORRECTIF`.

| Changement | Version | Conséquence |
|---|---|---|
| Règle assouplie, correction de description | correctif (1.0.**1**) | Aucun impact pour les consommateurs |
| Nouveau champ **optionnel**, nouvelle ville | mineur (1.**1**.0) | Schéma mis à jour **avant** que la source n'envoie le champ, sinon `additionalProperties: false` le rejette |
| Champ retiré ou renommé, type changé, règle durcie | majeur (**2**.0.0) | Prévenir les consommateurs ; rejouer le topic brut avec le nouveau validateur (`--replay`) |

La DLQ indique pour chaque rejet la version du contrat appliquée (`validator_version`). Après une correction du contrat, on peut donc retrouver les messages rejetés à tort, puis les retraiter.
