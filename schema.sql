-- ============================================
-- 競馬データベース スキーマ
-- 拡張性: 血統は最大6世代まで対応
-- ============================================

-- 血統条件別成績テーブル
-- generation: 1=父馬, 2=母父馬, 3=父父馬, 4=母母父馬, 5=父父父馬, 6=母父父馬
CREATE TABLE IF NOT EXISTS sire_stats (
    id              SERIAL PRIMARY KEY,
    sire_name       VARCHAR(100) NOT NULL,
    generation      SMALLINT NOT NULL DEFAULT 1,
    surface         CHAR(1) NOT NULL,
    distance        SMALLINT NOT NULL,
    place           VARCHAR(10),
    wins            INTEGER NOT NULL DEFAULT 0,
    second          INTEGER NOT NULL DEFAULT 0,
    third           INTEGER NOT NULL DEFAULT 0,
    out_count       INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sire_name, generation, surface, distance, place)
);

-- 騎手条件別成績テーブル
CREATE TABLE IF NOT EXISTS jockey_stats (
    id              SERIAL PRIMARY KEY,
    jockey_name     VARCHAR(50) NOT NULL,
    surface         CHAR(1) NOT NULL,
    distance        SMALLINT NOT NULL,
    place           VARCHAR(10),
    wins            INTEGER NOT NULL DEFAULT 0,
    second          INTEGER NOT NULL DEFAULT 0,
    third           INTEGER NOT NULL DEFAULT 0,
    out_count       INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (jockey_name, surface, distance, place)
);

-- レース結果テーブル
CREATE TABLE IF NOT EXISTS race_results (
    id              SERIAL PRIMARY KEY,
    race_id         VARCHAR(20) NOT NULL UNIQUE,
    race_date       DATE NOT NULL,
    place           VARCHAR(10) NOT NULL,
    surface         CHAR(1) NOT NULL,
    distance        SMALLINT NOT NULL,
    processed       BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_sire_stats_lookup
    ON sire_stats (sire_name, generation, surface, distance, place);
CREATE INDEX IF NOT EXISTS idx_jockey_stats_lookup
    ON jockey_stats (jockey_name, surface, distance, place);
CREATE INDEX IF NOT EXISTS idx_race_results_date
    ON race_results (race_date, processed);
