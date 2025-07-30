
CREATE SCHEMA IF NOT EXISTS etl_schema;
CREATE TABLE IF NOT EXISTS etl_schema.local_data (
    id SERIAL PRIMARY KEY,
    "DIA" INTEGER,
    "MES" INTEGER,
    "YEAR" INTEGER,
    "NUMERO DE ACEPTACION" VARCHAR(50),
    "ADUANA" VARCHAR(100),
    "RUT PROBABLE EXPORTADOR" VARCHAR(20),
    "DV PROBABLE EXPORTADOR" VARCHAR(5),
    "PARTIDA ARANCELARIA" VARCHAR(20),
    "PRODUCTO" TEXT,
    "VARIEDAD" VARCHAR(255),
    "MARCA" VARCHAR(255),
    "DESCRIPCION" TEXT,
    "PAIS DE DESTINO" VARCHAR(100),
    "VIA DE TRANSPORTE" VARCHAR(100),
    "COMPANIA DE TRANSPORTE" VARCHAR(255),
    "NAVE" VARCHAR(255),
    "TIPO DE CARGA" VARCHAR(100),
    "PUERTO EMBARQUE" VARCHAR(100),
    "PUERTO DESEMBARQUE" VARCHAR(100),
    "CLAUSULA DE VENTA" VARCHAR(50),
    "FORMA DE PAGO" VARCHAR(100),
    "PESO BRUTO" DECIMAL(15,3),
    "ITEM" INTEGER,
    "CANTIDAD" DECIMAL(15,3),
    "UNIDAD" VARCHAR(50),
    "US$ FOB" DECIMAL(15,2),
    "US$ FLETE" DECIMAL(15,2),
    "US$ SEGURO" DECIMAL(15,2),
    "US$ CIF" DECIMAL(15,2),
    "US$ FOB UNIT" DECIMAL(15,4),
    "TIPO DE BULTO" VARCHAR(100),
    "TIPO DE OPERACIÓN" VARCHAR(100),
    "DESCRIPCION ARANCELARIA" TEXT,
    "COMUNA EXPORTADOR" VARCHAR(100),
    "REGION DE ORIGEN" VARCHAR(100),
    "CANTIDAD DE BULTOS" INTEGER,
    "DESCRIPCIÓN DE BULTO" TEXT,
    "PAIS COMPAÑIA DE TRANSPORTE" VARCHAR(100),
    "MODALIDAD DE VENTA" VARCHAR(100),
    "TOTAL ITEMS" INTEGER,
    "PESO BRUTO TOTAL" DECIMAL(15,3),
    "TOTAL US$ FOB" DECIMAL(15,2),
    "ZONA ECONOMICA" VARCHAR(100),
    "CLAVE ECONOMICA" VARCHAR(20),
    "NUM DOC TRANSPORTE" VARCHAR(100),
    "FECHA DOC TRANSPORTE" VARCHAR(20),
    "NUM DE VIAJE" VARCHAR(100),
    "CANTIDAD UNIDADES FISICAS" DECIMAL(15,3),
    "UNIDAD DE MEDIDA FISICA" VARCHAR(50),
    "EMISOR" VARCHAR(255),
    "TOTAL US$ FLETE" DECIMAL(15,2),
    "TOTAL US$ SEGURO" DECIMAL(15,2),
    "TOTAL US$ CIF" DECIMAL(15,2),
    
    "SOURCE_FILE" VARCHAR(255),
    "EXTRACTION_DATE" TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_schema.world_bank_gdp (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3),
    country_name VARCHAR(255),
    indicator_code VARCHAR(50),
    indicator_name VARCHAR(255),
    year INTEGER,
    gdp_value DECIMAL(20,2),
    ranking INTEGER,
    gdp_growth_rate DECIMAL(8,4),
    gdp_billion_usd DECIMAL(15,3),
    log_gdp DECIMAL(10,6),
    region VARCHAR(50),
    economy_size VARCHAR(20),
    extraction_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_schema.export_analytics (
    id SERIAL PRIMARY KEY,
    analysis_type VARCHAR(50), 
    year INTEGER,
    month INTEGER,
    region VARCHAR(100),
    country VARCHAR(100),
    product VARCHAR(255),
    total_fob_usd DECIMAL(15,2),
    export_count INTEGER,
    avg_fob_usd DECIMAL(15,2),
    unique_exporters INTEGER,
    unique_destinations INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_schema.model_metadata (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    model_type VARCHAR(50), 
    training_date TIMESTAMP,
    accuracy_score DECIMAL(6,4),
    feature_names TEXT,
    model_path VARCHAR(255),
    data_period VARCHAR(20),
    countries_count INTEGER,
    training_samples INTEGER,
    test_samples INTEGER,
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_schema.model_predictions (
    id SERIAL PRIMARY KEY,
    model_id INTEGER REFERENCES etl_schema.model_metadata(id),
    model_type VARCHAR(50),
    country_code VARCHAR(3),
    year INTEGER,
    month INTEGER,
    category VARCHAR(100),
    predicted_value DECIMAL(20,2),
    actual_value DECIMAL(20,2),
    prediction_error DECIMAL(10,4),
    prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_local_data_year_month ON etl_schema.local_data("YEAR", "MES");
CREATE INDEX IF NOT EXISTS idx_local_data_country ON etl_schema.local_data("PAIS DE DESTINO");
CREATE INDEX IF NOT EXISTS idx_local_data_product ON etl_schema.local_data("PRODUCTO");
CREATE INDEX IF NOT EXISTS idx_local_data_region ON etl_schema.local_data("REGION DE ORIGEN");
CREATE INDEX IF NOT EXISTS idx_local_data_fob ON etl_schema.local_data("US$ FOB");
CREATE INDEX IF NOT EXISTS idx_local_data_extraction_date ON etl_schema.local_data("EXTRACTION_DATE");

CREATE INDEX IF NOT EXISTS idx_world_bank_gdp_country ON etl_schema.world_bank_gdp(country_code);
CREATE INDEX IF NOT EXISTS idx_world_bank_gdp_year ON etl_schema.world_bank_gdp(year);
CREATE INDEX IF NOT EXISTS idx_world_bank_gdp_ranking ON etl_schema.world_bank_gdp(ranking);
CREATE INDEX IF NOT EXISTS idx_world_bank_gdp_region ON etl_schema.world_bank_gdp(region);

CREATE INDEX IF NOT EXISTS idx_export_analytics_type_year ON etl_schema.export_analytics(analysis_type, year);
CREATE INDEX IF NOT EXISTS idx_model_predictions_model_type ON etl_schema.model_predictions(model_type, year);

CREATE OR REPLACE VIEW etl_schema.v_export_summary AS
SELECT 
    "YEAR",
    "MES",
    "PAIS DE DESTINO",
    "REGION DE ORIGEN",
    COUNT(*) as export_count,
    SUM("US$ FOB") as total_fob_usd,
    AVG("US$ FOB") as avg_fob_usd,
    SUM("PESO BRUTO") as total_weight_kg,
    COUNT(DISTINCT "RUT PROBABLE EXPORTADOR") as unique_exporters
FROM etl_schema.local_data
WHERE "US$ FOB" > 0
GROUP BY "YEAR", "MES", "PAIS DE DESTINO", "REGION DE ORIGEN";

CREATE OR REPLACE VIEW etl_schema.v_gdp_ranking_2024 AS
SELECT 
    country_code,
    country_name,
    gdp_value,
    gdp_billion_usd,
    ranking,
    region,
    economy_size,
    gdp_growth_rate
FROM etl_schema.world_bank_gdp
WHERE year = (SELECT MAX(year) FROM etl_schema.world_bank_gdp)
ORDER BY ranking;

INSERT INTO etl_schema.export_analytics (analysis_type, region, year, total_fob_usd, export_count, created_at)
VALUES 
    ('regional_reference', 'REGION METROPOLITANA', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE VALPARAISO', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DEL BIOBIO', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE LA ARAUCANIA', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE ANTOFAGASTA', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE ATACAMA', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE COQUIMBO', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DEL MAULE', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE LOS LAGOS', 2024, 0, 0, CURRENT_TIMESTAMP),
    ('regional_reference', 'REGION DE MAGALLANES', 2024, 0, 0, CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;