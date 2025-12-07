require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const URL_TOP100 = 'https://api.hisekai.org/event/live/top100';
const URL_BORDER = 'https://api.hisekai.org/event/live/border';

const TARGET_TOP_RANKS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
const TARGET_MID_RANKS = [200, 300, 400, 500, 1000, 1500, 2000, 2500, 3000, 5000, 10000];

async function fetchAndSave() {
    const now = new Date().toISOString();
    console.log(`[${now}] 啟動抓取任務...`);

    try {
        const [resTop100, resBorder] = await Promise.all([fetch(URL_TOP100), fetch(URL_BORDER)]);

        const dataTop100 = await resTop100.json();
        const dataBorder = await resBorder.json();

        const eventId = dataTop100?.id || 'unknown_event';

        let recordsToInsert = [];

        dataTop100.top_100_player_rankings.forEach((item) => {
            if (TARGET_TOP_RANKS.includes(item.rank)) {
                recordsToInsert.push({
                    rank: item.rank,
                    score: item.score,
                    event_id: eventId,
                    created_at: now,
                });
            }
        });

        dataBorder.border_player_rankings.forEach((item) => {
            if (TARGET_MID_RANKS.includes(item.rank)) {
                recordsToInsert.push({
                    rank: item.rank,
                    score: item.score,
                    event_id: eventId,
                    created_at: now,
                });
            }
        });

        if (recordsToInsert.length > 0) {
            const { error } = await supabase.from('event_rankings').insert(recordsToInsert);

            if (error) throw error;
            console.log(`成功儲存 ${recordsToInsert.length} 筆資料 (活動: ${eventId})`);
        } else {
            console.log('沒有資料需要儲存');
        }
    } catch (err) {
        console.error('抓取失敗:', err);
    }
}

fetchAndSave();
