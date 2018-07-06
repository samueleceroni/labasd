# SbornDB
In questo file sono racchiuse tutte le informazioni di progetto
# Strutture dati utilizzate
## AVL
Sono dei **BST** bilanciati. Ne utilizziamo uno per ogni campo della tabella che stiamo considernando. Siccome non creiamo indici al momento della creazione della tabella, dobbiamo poter trattare tutte le colonne come possibili indici.
## Tutto stringhe!
Tutti i valori dei campi nei record sono da trattare come **stringhe**, anche all'interno degli AVL. Un comparer si occupera' di gestire l'ordinamento logico dei valori (lessicografico per le stringhe, valore per i numeri).

# Strutture implementative
Utilizziamo le seguenti strutture per gestire i record delle tabelle

* Table
* Record
* AVLTree
* AVLNode

## Table
Contiene tutte le informazioni risuardanti una tabella
### Campi

* Nome
* Lista di campi (colonne della tabella)
* Lista di AVLTree (uno per ogni campo)
* Lista di Record

## Record
Rappresenta un record di una tabella.
### Campi

* Lista di valori (ognuno corrispondente a un campo)
* Tabella di riferimento

## AVLTree
Gestisce la ricerca dei record nella tabella.
Gestisce un solo campo (quindi un solo indice).
### Campi
* Nome campo gestito
* Struttura AVL di AVLNode

## AVLNode
Rappresenta un nodo dell'AVLTree
### Campi

* Chiave (sempre striga!!!)
* Linked list di puntatori agli elementi con la stessa chiave del nodo del record
* Contatore (in particolare la lunghezza della lista descritta sopra)

# Gestione Creazione Tabelle
***TODO***

# Gestione Inserimento Record
***TODO***

# Gestione Filtri
## WHERE
Ci sono due tipi di ricerche con WHERE:

* Puntuali
* Range

### Puntuali
Ricerchiamo la chiave nell'AVL e ritorniamo i record associati.
### Range
Ricerchiamo la chiave nell'AVL, otteniamo tutti i nodi precedenti/successivi (compreso quello ricercato se il range e' inclusivo), concateniamo le liste di record ottenute e ritorniamo la concatenazione.

## ORDER BY
Stampiamo tutto l'AVL (in baso all'ordine da sx a dx o inverso), concatenando le liste di record.
## GROUP BY
Stampiamo tutti i nodi dell'AVL con il loro contatore
