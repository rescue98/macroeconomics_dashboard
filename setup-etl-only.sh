#!/bin/bash
# setup-etl-only.sh

set -e

echo "🚀 Configurando Pipeline ETL - Airflow + Spark + MinIO + PostgreSQL"
echo "=============================================================================="

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar prerrequisitos
check_prerequisites() {
    log_info "Verificando prerrequisitos..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose no está instalado."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker no está corriendo."
        exit 1
    fi
    
    log_success "Prerrequisitos OK"
}

# Crear estructura mínima
create_directories() {
    log_info "Creando directorios para ETL..."
    
    directories=(
        "dags"
        "spark_jobs"
        "data"
        "logs"
        "plugins"
        "sql"
    )
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        log_info "  ✓ $dir"
    done
    
    log_success "Directorios creados"
}

# Verificar datos reales
check_real_data() {
    log_info "Verificando datos reales de exportaciones..."
    
    if [ ! -d "data" ]; then
        log_error "Directorio 'data/' no existe"
        log_error "Por favor crea el directorio 'data/' y coloca tus archivos CSV de exportaciones ahí"
        exit 1
    fi
    
    # Buscar archivos CSV
    csv_count=$(find data/ -name "*.csv" -type f | wc -l)
    
    if [ $csv_count -eq 0 ]; then
        log_error "No se encontraron archivos CSV en el directorio 'data/'"
        log_error ""
        log_error "Por favor coloca tus archivos CSV de exportaciones en el directorio 'data/'"
        log_error "Ejemplo:"
        log_error "  data/exportaciones_2022.csv"
        log_error "  data/exportaciones_2023.csv" 
        log_error "  data/exportaciones_2024.csv"
        log_error ""
        log_error "Los archivos deben contener las columnas de exportaciones chilenas:"
        log_error "  - US\$ FOB"
        log_error "  - PAIS DE DESTINO"
        log_error "  - PRODUCTO"
        log_error "  - REGION DE ORIGEN"
        log_error "  - etc."
        exit 1
    fi
    
    log_success "Encontrados $csv_count archivos CSV"
    
    # Listar archivos encontrados
    log_info "Archivos CSV detectados:"
    find data/ -name "*.csv" -type f | while read file; do
        size=$(du -h "$file" | cut -f1)
        log_info "  ✓ $file ($size)"
    done
}

start_etl_services() {
    log_info "Iniciando servicios ETL..."
    
    log_info "  → PostgreSQL y MinIO..."
    docker-compose up -d postgres minio
    sleep 10
    
    log_info "  → Spark cluster..."
    docker-compose up -d spark-master spark-worker
    sleep 10
    
    log_info "  → Airflow..."
    docker-compose up -d redis airflow-webserver airflow-scheduler
    sleep 20
    
    log_success "Servicios ETL iniciados"
}

check_etl_services() {
    log_info "Verificando servicios ETL..."
    
    etl_services=("postgres" "minio" "spark-master" "spark-worker" "airflow-webserver" "airflow-scheduler")
    
    for service in "${etl_services[@]}"; do
        if docker-compose ps | grep -q "${service}.*Up"; then
            log_success "  ✓ $service corriendo"
        else
            log_warning "  ⚠ $service con problemas"
        fi
    done
}

show_etl_info() {
    echo ""
    echo "🌐 ACCESO A SERVICIOS ETL"
    echo "========================="
    echo ""
    echo -e "${GREEN}Airflow ETL:${NC}           http://localhost:8081"
    echo -e "${BLUE}  Usuario: admin / admin${NC}"
    echo ""
    echo -e "${GREEN}Spark Master:${NC}          http://localhost:8080"
    echo ""
    echo -e "${GREEN}MinIO Console:${NC}         http://localhost:9001"
    echo -e "${BLUE}  Usuario: minioadmin / minioadmin123${NC}"
    echo ""
    echo -e "${GREEN}PostgreSQL:${NC}            localhost:5432"
    echo -e "${BLUE}  DB: etl_database, User: etl_user${NC}"
    echo ""
}

show_etl_steps() {
    echo "📋 SOLO ETL"
    echo "============================"
    echo ""
    echo "1. Configurar conexión PostgreSQL en Airflow:"
    echo "   - http://localhost:8081 → Admin → Connections"
    echo "   - Conn Id: postgres_default"
    echo "   - Host: postgres, Schema: etl_database"
    echo "   - User: etl_user, Pass: etl_password"
    echo ""
    echo "2. Ejecutar SOLO estos 2 DAGs:"
    echo "   ✅ etl_local_csv_pipeline (tus datos de exportaciones)"
    echo "   ✅ etl_worldbank_gdp_pipeline (World Bank GDP 2024)"
    echo ""
    echo "3. Verificar resultados en:"
    echo "   - MinIO: http://localhost:9001 (archivos procesados)"
    echo "   - PostgreSQL: datos cargados en tablas"
    echo ""
    echo "4. Para verificar datos cargados:"
    echo "   docker-compose exec postgres psql -U etl_user -d etl_database"
    echo "   SELECT COUNT(*) FROM etl_schema.local_data;"
    echo "   SELECT COUNT(*) FROM etl_schema.world_bank_gdp;"
    echo ""
    echo "❌ NO ejecutar DAGs de ML (model_training_*)"
    echo ""
}

main() {
    check_prerequisites
    create_directories
    check_real_data
    start_etl_services
    
    log_info "Esperando estabilización..."
    sleep 30
    
    check_etl_services
    show_etl_info
    show_etl_steps
    
    echo ""
    log_success "ETL CONFIGURADO - LISTO PARA PROCESAR DATOS REALES"
    echo ""
}

main