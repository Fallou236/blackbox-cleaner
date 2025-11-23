# 🧹 Blackbox Cleaner — Script de Nettoyage et d’Optimisation des Données

Ce projet contient un script Python avancé permettant de **charger, nettoyer, fusionner et anonymiser** des données issues de fichiers JSON hétérogènes. Il a été spécialement pensé pour la compétition Zindi / FORCE-N Data Engineering, mais peut être utilisé dans tout autre contexte de data cleaning.

---

## 🚀 Fonctionnalités principales

### ✔️ Chargement JSON flexible
Le script supporte :
- JSON classiques (dict, liste)
- JSON complexes/normaux
- JSON ligne-par-ligne (NDJSON)
- JSON corrompus (récupération des lignes valides)

### ✔️ Nettoyage complet
- Suppression/masquage automatique des **données sensibles (PII)**
    - Emails masqués
    - Identifiants nationaux masqués
    - Numéros de téléphone anonymisés
    - Notes internes expurgées
- Correction automatique des **dates** → format *JJ/MM/AAAA HH:MM:SS*
- Détection heuristique des types (date, PII, numérique…)
- Arrondi des valeurs numériques à 2 décimales

### ✔️ Fusion intelligente
- Fusion automatique entre `users.json` et `transactions.json`
- Détection automatique de la clé de jointure (`user_id`, `customer_id`, etc.)
- Les colonnes manquantes sont ajoutées proprement

### ✔️ Normalisation
- La colonne **ID** devient toujours la première colonne
- Toutes les colonnes sont converties en texte afin d’assurer un CSV propre

### ✔️ Export propre
- Le résultat final est exporté en **CSV encodé UTF‑8**

---

## 📁 Structure du projet
```
blackbox_cleaner/
│
├── blackbox_cleaner.py        # Script principal (nettoyage + fusion + anonymisation)
├── README.md                  # Documentation du projet
├── output/                    ## Dossier de sortie du CSV nettoyé 
└── Data/                      # Données users.json / transactions.json (optionnel)
```

---

## 🛠️ Installation
Assurez‑vous d’avoir Python 3.8+.

### 1. Installer les dépendances
```bash
pip install pandas numpy python-dateutil
```

---

## 🧪 Utilisation

### 📘 Exemple complet d’utilisation
Voici un exemple simple montrant comment appeler la fonction `clean_data` avec deux fichiers JSON bruts, puis récupérer le CSV nettoyé :

```python
from blackbox_cleaner import clean_data

# chemins vers vos fichiers JSON
users_file = "data/users.json"
transactions_file = "data/transactions.json"
output_file = "clean_output.csv"

# exécution du nettoyage
cleaned_df = clean_data(users_file, transactions_file, output_file)

print("Nettoyage terminé ! CSV généré :", output_file)
print("Aperçu du DataFrame nettoyé :")
print(cleaned_df.head())
```

Utilisez simplement :
Le script expose une fonction principale :
```
clean_data(users_path, transactions_path, output_path)
```

### Exemple :
```bash
python3
>>> from blackbox_cleaner import clean_data
>>> clean_data("users.json", "transactions.json", "output_clean.csv")
```

Le fichier **output_clean.csv** contiendra :
- données fusionnées
- dates normalisées
- PII masquées
- types unifiés
- identifiants propres

---

## 🔒 Sécurité & Anonymisation (PII)
Le script masque automatiquement :
- emails → `n****@domain`
- identifiants nationaux → `ABCXXXXXXX`
- numéros de téléphone → entièrement anonymisés
- notes internes → chiffres et emails supprimés

Cela permet d’éviter toute fuite de données sensibles.

---

## 📜 Licence
Projet open‑source sous licence **MIT**. Vous êtes libre de le modifier, améliorer ou réutiliser.

---

## ✨ Auteur
Projet développé par **Fallou Diouck**, Data Engineer / Software Engineer.

N’hésite pas à ouvrir une *issue* si tu veux améliorer quelque chose ou ajouter une fonctionnalité 🙌

