# onsight-jobs — Background Task Manager & Persistent Job Queue

Pacchetto condiviso da `onsight-backend` e `onsight-async`: **unica sorgente** del `BackgroundTaskManager` e delle query su `public.background_jobs`. Prima ognuno dei due repo ne aveva una copia e le copie divergevano; ora una modifica si fa qui, si rilascia con un tag e i due servizi aggiornano la dipendenza.

## Installazione

Il repository è pubblico: non serve nessun token. I consumer lo dichiarano come dipendenza git fissata a un tag:

```json
"dependencies": {
  "onsight-jobs": "git+https://github.com/onsight-climb/onsight-jobs.git#v1.0.1"
}
```

```typescript
import { BackgroundTaskManager } from "onsight-jobs";
// Query di basso livello: import { popNextBackgroundJobQuery } from "onsight-jobs/queries";
// Helper solo per i test (DELETE per pattern): import { cleanBackgroundJobsTableQuery } from "onsight-jobs/testing";
```

Il pacchetto esporta sorgenti TypeScript (Bun li esegue direttamente) e usa la connessione `sql` di default di Bun (`DATABASE_URL`).

## Rilascio

1. Modifica + test (`bun run typecheck`, `bun run test:unit`, `bun run test:sql` con `DATABASE_URL` su un DB che ha `test/schema.sql`).
2. Aggiorna `version` in `package.json`, tag `vX.Y.Z`, push del tag.
3. In ciascun consumer aggiorna il `#vX.Y.Z` e rigenera `bun.lock`.

Lo schema della tabella resta di proprietà delle migrazioni Supabase di `onsight-backend`; `test/schema.sql` ne è una replica minima solo per la CI di questo pacchetto.

Il `BackgroundTaskManager` è il motore di gestione dei task asincroni non bloccanti di **Onsight**. È progettato per liberare immediatamente il thread delle API HTTP eseguendo operazioni lente (come notifiche push, email o analisi statistiche) in background.

Il sistema supporta due modalità operative principali:

1. **In-Memory Queue (Volatile)**: Ideale per task rapidi, non critici e che non richiedono di sopravvivere ai riavvii del server.
2. **Persistent Queue (Persistente su DB)**: Basata su **Bun SQL** e **PostgreSQL**, ideale per task mission-critical che devono sopravvivere a crash, aggiornamenti software o riavvii del server.

---

## Indice

1. [Architettura e Vantaggi](#architettura-e-vantaggi)
2. [Schema Database](#schema-database)
3. [Guida per lo Sviluppatore](#guida-per-lo-sviluppatore)
   - [Inizializzazione del Worker](#1-inizializzazione-del-worker)
   - [Registrazione di un Runner (Step 1)](#2-registrazione-di-un-runner-step-1)
   - [Accodamento di un Job (Step 2)](#3-accodamento-di-un-job-step-2)
4. [Gestione dei Task In-Memory Volatili](#gestione-dei-task-in-memory-volatili)
5. [Graceful Shutdown (Spegnimento Sicuro)](#graceful-shutdown-spegnimento-sicuro)
6. [Best Practice](#best-practice)

---

## Architettura e Vantaggi

La coda persistente sfrutta funzionalità avanzate di PostgreSQL per garantire performance di livello enterprise e totale coerenza dei dati:

- **SKIP LOCKED (Concurrency Control)**: La query di polling usa `FOR UPDATE SKIP LOCKED`. Questo permette a più istanze dell'applicazione (es. in ambienti multi-nodo o serverless) di interrogare la coda simultaneamente senza che due nodi elaborino mai lo stesso task (nessun duplicato e nessuna race condition).
- **Backpressure**: Se il server è sovraccarico (task in esecuzione ≥ `maxConcurrency`, default 50), il polling asincrono da database viene messo in pausa finché non si liberano slot in memoria, proteggendo la CPU del server.
- **Backoff Esponenziale**: In caso di errore di esecuzione, il manager ricalcola automaticamente il tempo del prossimo tentativo incrementandolo in modo esponenziale per evitare di sovraccaricare le terze parti (es. Firebase o provider email momentaneamente down).

---

## Schema Database

I job persistenti risiedono nella tabella `background_jobs`.

- **`status`**: Può essere `'pending'`, `'running'`, `'completed'`, o `'failed'`.
- **`run_at`**: Timestamp UTC in cui il task è programmato per l'esecuzione.
- **`attempts`**: Il conteggio corrente dei tentativi eseguiti.
- **`max_retries`**: Il numero massimo di retry consentiti prima di dichiarare il task definitivamente fallito (`'failed'`).
- **`error_message`**: Log dell'ultimo errore riscontrato per facilitare il debug. Vale `Interrupted by shutdown` per un job restituito alla coda da uno spegnimento (vedi sotto).
- **`run_at`** (anche per i retry in backoff) viene calcolato dal database (`now() at time zone 'utc'` + ritardo), mai dall'orologio dell'host: il poller lo confronta con il `now()` del DB e uno sfasamento tra i due orologi renderebbe un job immediato "non ancora dovuto".

---

## Guida per lo Sviluppatore

### 1. Inizializzazione del Worker

Il worker di polling viene avviato all'avvio dell'applicazione, non appena la connessione al database viene verificata con successo. Una volta avviato continua a interrogare il DB finché non viene fermato esplicitamente (`stopWorker()`/`shutdown()`): non c'è auto-stop per inattività, perché un risultato vuoto può nascondere job ritardati, retry in backoff o righe inserite dall'altro servizio. Un processo che si limita ad accodare (senza `startWorker()`) non avvia mai il worker implicitamente.

```typescript
import { BackgroundTaskManager } from "onsight-jobs";

// Avvia il polling worker (default: controlla il DB ogni 5 secondi)
BackgroundTaskManager.startWorker();
```

---

### 2. Registrazione di un Runner (Step 1)

Un **Runner** è la funzione TypeScript che esegue l'operazione di business logic. I runner devono essere registrati all'avvio dell'applicazione prima che il worker parta. Il posto migliore è il costruttore del relativo servizio o all'interno del file di avvio.

Backend e `onsight-async` condividono la tabella `background_jobs`. Ogni processo preleva e recupera soltanto i job dei runner registrati localmente: gli altri restano disponibili per il processo competente, senza consumare tentativi. Ad esempio, `ranking-camps-maintenance` è registrato in `onsight-async` e viene eseguito lì. Un job senza runner disponibile resta `pending`.

Il runner riceve due argomenti:

1. `payload`: Il plain object contenente i dati del task (deserializzato automaticamente da JSONB).
2. `signal`: Un `AbortSignal` utile per interrompere le operazioni in corso (es. fetch o query SQL) se scatta un timeout o se il server si spegne.

#### Esempio:

```typescript
// In src/gym/service.ts (Costruttore)
import { z } from "zod";
import { BackgroundTaskManager } from "onsight-jobs";
import { sendPushNotificationToMultipleUsers } from "../utils/notifications.js";

// Il payload arriva come `unknown` dal JSONB: va validato prima dell'uso.
const AdminSuggestionPayload = z.object({
  title: z.string(),
  body: z.string(),
  deeplink: z.string(),
  adminUserIds: z.array(z.string()),
});

export class GymService extends BaseGymService {
  constructor() {
    super();

    // Registrazione del runner dedicato alle notifiche amministrative
    BackgroundTaskManager.registerRunner(
      "send-admin-gym-suggestion-notification",
      async (payload, signal) => {
        const { title, body, deeplink, adminUserIds } =
          AdminSuggestionPayload.parse(payload);
        if (signal.aborted) return;

        // Restituisce { notifiedUserIds, sentCount, failureCount }: gli errori FCM non
        // vengono rilanciati (la voce in-app esiste già e un retry la duplicherebbe).
        const { sentCount, failureCount } =
          await sendPushNotificationToMultipleUsers(
            adminUserIds,
            title,
            body,
            "general",
            { deeplink },
          );
        console.log(`Notifiche admin: ${sentCount} inviate, ${failureCount} fallite`);
      },
    );
  }
}
```

---

### 3. Accodamento di un Job (Step 2)

Una volta registrato il runner, puoi accodare ed eseguire un task in qualsiasi parte dei tuoi servizi usando `BackgroundTaskManager.enqueue()`.

```typescript
// All'interno di una API o di un Servizio
await BackgroundTaskManager.enqueue(
  "send-admin-gym-suggestion-notification", // Nome del runner precedentemente registrato
  {
    title: "Nuovo Suggerimento Palestra",
    body: "Un utente ha suggerito una nuova palestra.",
    metadata: { deeplink: "/dashboard/suggestions" },
    userId: user_id,
  },
  {
    delayMs: 0, // Opzionale. Ritardo in ms prima dell'esecuzione (0 = immediato)
    retries: 3, // Opzionale. Numero di tentativi in caso di fallimento (default: 0)
    initialDelayMs: 2000, // Opzionale. Ritardo iniziale per il retry esponenziale in ms (default: 1000)
  },
);
```

> [!IMPORTANT]  
> L'invocazione di `enqueue()` scrive direttamente sulla tabella del database Postgres tramite una query Bun SQL pre-connessa ed estremamente veloce. È consigliabile chiamare `await BackgroundTaskManager.enqueue(...)` per garantire che il job sia registrato su DB prima di rispondere con successo al client HTTP.

---

## Gestione dei Task In-Memory Volatili

Per operazioni non critiche (es. tracciamento analytics a fini statistici) in cui non è grave perdere il task in caso di spegnimento del server, puoi usare la coda volatile in memoria:

```typescript
BackgroundTaskManager.run(
  async (signal) => {
    // Logica asincrona veloce
    await trackAnalyticsEvent("gym_suggestion_clicked", { userId });
  },
  {
    name: "track-gym-suggestion-click", // Nome descrittivo per log e cancellazioni
    timeoutMs: 15000, // Timeout di esecuzione (default: 30s)
    retries: 1, // Tentativi in-memory in caso di errore (default: 0)
  },
);
```

### Cancellare o Controllare Task In-Memory

Puoi controllare lo stato o cancellare task in-memory pendenti o in esecuzione:

```typescript
// Controlla se esiste un task attivo o in coda
if (BackgroundTaskManager.has("track-gym-suggestion-*")) {
  // Cancella programmaticamente i task corrispondenti
  BackgroundTaskManager.cancel("track-gym-suggestion-*");
}
```

---

## Graceful Shutdown (Spegnimento Sicuro)

La sequenza è implementata una sola volta in `BackgroundTaskManager.startDrain(graceMs)`, che restituisce `{ activeCount, done }` (`done` risolve `{ timedOut }`):

1. *(solo consumer con altri poller, es. `onsight-async`)* **Stop polling cron** (`CronjobManager.shutdown()`) prima di chiamare `startDrain`.
2. **Stop polling della coda** e flag di shutdown: i nuovi task vengono rifiutati. Un job acquisito da un tick già in volo viene restituito alla coda anziché perso.
3. **Drain**: attende `waitForAll()` fino a `graceMs`, così i job brevi terminano e risultano `completed`.
4. **Abort**: `shutdown()` emette l'`AbortSignal` ai task rimasti.
5. Il consumer ferma il server HTTP e il processo esce.

```typescript
const { done } = BackgroundTaskManager.startDrain(SHUTDOWN_GRACE_MS);
const { timedOut } = await done;
```

Un job persistente interrotto allo step 4 **non fallisce**: il wrapper chiama `requeueInterruptedJobQuery(id)` e lo riporta a `status='pending'`, `attempts = max(attempts - 1, 0)`, `run_at = now()`, `started_at = NULL`, `error_message = 'Interrupted by shutdown'`. Il prossimo avvio del worker lo riesegue. Serve soprattutto ai job dei cron (`max_retries = 0`), la cui riga `cron_jobs` ha già avanzato `next_run_at`: senza requeue l'occorrenza andrebbe persa. Un abort per **timeout** (non per shutdown) mantiene il comportamento di retry/fail.

I runner devono quindi rispettare `signal` e restare idempotenti: un job interrotto viene rieseguito dall'inizio.

Job acquisiti ma non ancora conclusi allo spegnimento (runner che ignora `signal`, o job ancora nella coda in memoria oltre `maxConcurrency`) non restano `running`: `shutdown()` attende al massimo `shutdownHardTimeoutMs` (default 5000 ms, `configure()`) e poi li restituisce alla coda con lo stesso `requeueInterruptedJobQuery`. Anche un tick di polling in volo viene atteso, così un job appena acquisito viene restituito prima dell'uscita. Solo un `SIGKILL` (o un crash) lascia righe `running`: le recupera `resetStuckRunningJobsQuery` al successivo `startWorker()`, dopo `timeout_ms` + 10 s.

Su Railway imposta il *draining time* del deployment ad almeno `SHUTDOWN_GRACE_MS` + 5 secondi, altrimenti il `SIGKILL` arriva prima dell'abort e del requeue.

---

## Best Practice

1. **Gestisci l'AbortSignal nei tuoi servizi**: Quando scrivi servizi di terze parti (invio email, notifiche, integrazioni payment), accetta sempre un parametro `AbortSignal` e passalo alle chiamate downstream (es. a `fetch()` o a query a database). Questo assicura che in caso di shutdown o timeout, le risorse vengano liberate istantaneamente.
2. **Utilizza la coda persistente per operazioni monetarie o notifiche**: Tutto ciò che compromette la User Experience (come conferme ordine, push alert, invio password reset) deve usare `.enqueue()` persistente.
3. **Evita Payload giganteschi**: Il payload viene memorizzato come colonna `JSONB`. Evita di salvare interi oggetti complessi o file binari; memorizza invece gli ID dei record (es. `user_id`, `gym_id`) ed esegui le query all'interno del runner per caricare i dati aggiornati al momento del run.
4. **Isolamento dei Test**: Nei file di test (`.test.ts`), usa sempre nomi di job con prefisso dedicato (es. `test-persistent-%`) ed effettua la pulizia del database all'inizio ed alla fine di ogni suite usando `cleanBackgroundJobsTableQuery()`.

## TypeScript e payload persistiti

I runner ricevono `payload: unknown`: validare il JSON con lo schema del task
prima di leggere proprietà o produrre effetti esterni. `enqueue` accetta
`Record<string, unknown>`; i valori devono restare serializzabili in JSON.
Nome del runner e chiavi del payload restano compatibili con i job già accodati.
La tipizzazione non cambia filtri di proprietà, lock, timeout, retry o recupero.

Le righe lette dalle query hanno il contratto `BackgroundJob` in [src/model.ts](src/model.ts);
una ricerca senza risultati restituisce `null`. I test devono verificare
esplicitamente l'esistenza delle fixture prima di accedere alle proprietà.
