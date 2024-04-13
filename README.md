# README
# ALDICA MARIA-MIRUNA, 331CA

Tema mea folosește un pool de threaduri pentru a răspunde diverselor requesturi
definite în routes.
Principul de funcționare pentru fiecare fișier în parte este următorul:

## data_ingestor.py
În data_ingestor am citit datele din baza mea de date, folosindu-mă
de modul de citire din https://docs.python.org/3/library/csv.html.

## __init__.py
În __init__.py am realizat directorul results pentru afișarea outputurilor.

## routes.py
În fișierul routes.py, am definit în mod global job_id-ul care pleacă de la
valoarea 0 si pentru care am realizat un Lock(), pentru a face sincronizarea.
De fiecare dată când realizez ruta pentru un request și cresc job id-ul,
înainte mă asigur că nici un alt thread nu accesează resursa partajată
prin folosirea lock-ului definit anterior. De asemenea, în acest timp
apelez un contructor denumit sugestiv job pentru a adăuga noul job în 
coada de taskuri, folosind în același timp parametrii sugestivi, precum
numărul id-ului, comanda, întrebarea și de la caz la caz statul, pe care
îi voi folosi mai târziu.

În ruta pentru get_results, la fel, m-am folosit de un alt lock pentru
a mă asigura ca doar un singur thread accesează resursa partajată în
cazul în care jobul meu este terminat, dar și dacă el este în desfășurare
și de la caz la caz am afișat un json cu un mesaj de running sau unul
cu datele asociate jobului. Dacă niciuna dintre condiții nu este 
îndeplinită, vom afișa un răspuns json care indică ca jobul este invalid.

În graceful_shutdown, când accesăm, se apeleză metoda shutdown din
cadrul ThreadPool-ului, pe care o voi detalia mai târziu.

În jobs pur și simplu am verificat fiecare job în parte și dacă a
fost terminat sau în desfășurare, am afișat un răspuns json evident.
În schimb, pentru num_jobs, pur și simplu am calculat numărul
de joburi în running și am afișat corespunzător. 

## task_runner.py
În task_runner.py am început prin a-mi defini clasa Job, despre care am vorbit
mai sus. În ThreadPool, am pornit un număr de fire de execuție echivalent cu
self.max_threads, iar pentru fiecare se inițializează un nou obiect TaskRunner,
se adaugă în lista self.threads, apoi fiecare TaskRunner este pornit cu start. 

În metoda shutdown am pus None în coada de taskuri, pentru a semnala fiecărui
fir de execuție că trebuie să se oprescă. După ce toate au fost atenționate
că trebuie să se oprească, apelăm join.

În TaskRunner, începem prin a lua câte un element din coada de taskuri și pentru
acesta verficăm comanda corespunzătoare și pentru fiecare în parte apelăm metoda
corespunzătoare pentru a obține rezultatul favorabil, pe care îl voi scrie în
fișierul de output corespunzător.. Toate aceste metode sunt implementate în cadrul 
clasei TaskRunner. Înainte de a apela metoda corespunzătoare jobului, acesta se află 
în starea de running, iar imediat după se află în starea de finished. Pentru 
sincronizare, m-am folosit din nou de lock-uri.

# unittests
Pentru unitesturi, am procedat în următorul fel: Mi-am construit o bază de date
suficient de diversă astfel încât să pot trata fiecare request în parte, pentru care
am pus în fișierele de in comenzile considerate de mine ca fiind relevante,
iar în fișierul de output, am pus rezultatele calculate anterior de mine folosindu-mă de metodele pe care le-am implementat în TaskRunner. 
În TestWebServer.py am luat direct codul din checker pe care l-am adaptat
la calea mea nouă și la noile modificări, am luat codul integral, mai 
puțin metoda pentru coding style, deoarece nu era relevantă absolut deloc.
FOARTE IMPORTANT!!!! Pentru testare, am înlocuit în task_runner.py 
linia 51 (self.data = DataIngestor('./nutrition_activity_obesity_usa_subset.csv').data)
cu self.data = DataIngestor('./unittests/teste.csv').data,
ca să îmi facă calculele conform noii baze de date.

## Concluzii finale
Consider că tema a fost un prilej benefic pentru a-mi fixa și
mai bine noțiunile de sincronizare.