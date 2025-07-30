// Команди для налаштування середовища
//
// # Створити датасет у BigQuery
// bq --location=${REGION} mk --dataset ${PROJECT_ID}:${DATASET}
//
// # Створити таблицю у BigQuery
// bq mk --table \
//   ${PROJECT_ID}:${DATASET}.quiz_leads \
//   email:STRING,asset_type:STRING,asset_value:STRING,goal:STRING,deadline:STRING,docs_ready:STRING,created_at:TIMESTAMP,user_agent:STRING,utm_source:STRING,utm_medium:STRING,utm_campaign:STRING
//
// # Деплой функції
// gcloud functions deploy submitQuiz \
//   --gen2 \
//   --trigger-http \
//   --runtime=nodejs20 \
//   --region=${REGION} \
//   --source=. \
//   --entry-point=submitQuiz \
//   --allow-unauthenticated \
//   --set-env-vars PROJECT_ID=${PROJECT_ID},DATASET=${DATASET},SECRET_TOKEN=${SECRET_TOKEN},ALLOWED_ORIGIN=${ALLOWED_ORIGIN}

const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

const {
    PROJECT_ID,
    DATASET,
    SECRET_TOKEN,
    ALLOWED_ORIGIN
} = process.env;

const TABLE = 'quiz_leads';

exports.submitQuiz = async (req, res) => {
    // Встановлюємо заголовки CORS
    res.set('Access-Control-Allow-Origin', ALLOWED_ORIGIN || "*");
    res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.set('Access-Control-Allow-Headers', 'Content-Type, X-Secret');

    // Обробка запиту OPTIONS для pre-flight
    if (req.method === 'OPTIONS') {
        return res.status(204).send('');
    }

    // Перевірка методу запиту
    if (req.method !== 'POST') {
        return res.status(405).send('Method Not Allowed');
    }

    // Перевірка секретного токену
    const providedSecret = req.get('X-Secret');
    if (!providedSecret || providedSecret !== SECRET_TOKEN) {
        console.warn('Invalid or missing secret token.');
        return res.status(403).json({ status: 'error', message: 'Forbidden' });
    }

    // Отримання та валідація даних
    const data = req.body;
    if (!data || typeof data.email !== 'string') {
        console.warn('Invalid payload: email is missing.');
        return res.status(400).json({ status: 'error', message: 'Bad Request: email is required.' });
    }

    const row = {
        email: data.email,
        asset_type: data.asset_type || null,
        asset_value: data.asset_value || null,
        goal: data.goal || null,
        deadline: data.deadline || null,
        docs_ready: data.docs_ready || null,
        created_at: data.created_at ? new Date(data.created_at).toISOString() : new Date().toISOString(),
        user_agent: data.user_agent || req.get('User-Agent') || null,
        utm_source: data.utm_source || null,
        utm_medium: data.utm_medium || null,
        utm_campaign: data.utm_campaign || null,
    };

    try {
        // Запис даних у BigQuery
        await bigquery
            .dataset(DATASET)
            .table(TABLE)
            .insert(row);
        
        console.log(`Successfully inserted lead for ${row.email}`);
        res.status(200).json({ status: 'ok' });

    } catch (error) {
        console.error('BigQuery Insert Error:', error);
        if (error.errors) {
            error.errors.forEach(err => console.error(JSON.stringify(err, null, 2)));
        }
        res.status(500).json({ status: 'error', message: 'Internal Server Error' });
    }
};
