00_setup.sh : créer deux canaux dans kafka
    - 1: events 
    - 2: events-stats

01_producer.py simule notre e-commerce, visiteurs, clicks, achats

02_consumer.py lit le fichier producer

03_processor.py lit un flux de click, regroupe flux tous les 10sec

04_sink_sqlite.py Lit les resumé du processus et persiste les données 

05_query.py interroge la ddb et affiche les bilans