#!/bin/bash

# Setup script para el pipeline ETL completo
# Autor: Asistente IA
# Versión: 1.0

set -e  # Exit on any error

echo "🚀 Configurando Pipeline ETL - Airflow + Spark + MinIO + PostgreSQL"
echo "=================================================================="

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para mostrar mensajes
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
    
    # Verificar Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado. Por favor instalar Docker primero."
        exit 1
    fi
    
    # Verificar Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose no está instalado. Por favor instalar Docker Compose primero."
        exit 1
    fi
    
    # Verificar que Docker esté corriendo
    if ! docker info &> /dev/null; then
        log_error "Docker no está corriendo. Por favor iniciar Docker primero."
        exit 1
    fi
    
    log_success "Todos los prerrequisitos están instalados"
}

# Crear estructura de directorios
create_directory_structure() {
    log_info "Creando estructura de directorios..."
    
    directories=(
        "dags"
        "spark_jobs"
        "models"
        "streamlit_app/utils"
        "data"
        "logs"
        "plugins"
        "sql"
        "notebooks"
        "minio/data/raw"
        "minio/data/processed"
        "minio/data/models"
    )
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        log_info "  ✓ Creado directorio: $dir"
    done
    
    log_success "Estructura de directorios creada"
}

# Configurar permisos
setup_permissions() {
    log_info "Configurando permisos..."
    
    # Crear directorio de logs y darle permisos apropiados para Airflow
    sudo chown -R 50000:0 logs/ 2>/dev/null || {
        log_warning "No se pudieron cambiar permisos de logs/. Continuando..."
    }
    
    # Dar permisos de ejecución a scripts
    chmod +x generate_sample_data.py 2>/dev/null || true
    chmod +x setup.sh 2>/dev/null || true
    
    log_success "Permisos configurados"
}

# Generar datos de ejemplo
generate_sample_data() {
    log_info "Verificando datos CSV..."
    
    csv_files=("08_2019.csv" "08_2020.csv" "08_2021.csv" "08_2022.csv" "08_2023.csv")
    missing_files=()
    
    for file in "${csv_files[@]}"; do
        if [ ! -f "data/$file" ]; then
            missing_files+=("$file")
        fi
    done
    
    if [ ${#missing_files[@]} -gt 0 ]; then
        log_warning "Faltan archivos CSV: ${missing_files[*]}"
        log_info "Generando datos de ejemplo..."
        
        if command -v python3 &> /dev/null; then
            python3 generate_sample_data.py
            log_success "Datos de ejemplo generados"
        else
            log_error "Python3 no está disponible. Por favor:"
            log_error "1. Instalar Python3"
            log_error "2. Ejecutar: python3 generate_sample_data.py"
            log_error "3. O colocar manualmente los archivos CSV en el directorio data/"
            exit 1
        fi
    else
        log_success "Todos los archivos CSV están presentes"
    fi
}

# Verificar archivos de configuración
check_config_files() {
    log_info "Verificando archivos de configuración..."
    
    required_files=(
        "docker-compose.yml"
        ".env"
        "Dockerfile"
        "Dockerfile.streamlit"
        "requirements.txt"
        "requirements_streamlit.txt"
    )
    
    missing_files=()
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            missing_files+=("$file")
        fi
    done
    
    if [ ${#missing_files[@]} -gt 0 ]; then
        log_error "Faltan archivos de configuración: ${missing_files[*]}"
        log_error "Por favor asegúrate de que todos los archivos estén en el directorio actual"
        exit 1
    fi
    
    log_success "Todos los archivos de configuración están presentes"
}

# Verificar puertos disponibles
check_ports() {
    log_info "Verificando puertos disponibles..."
    
    ports=(5432 8080 8081 8501 9000 9001 6379 7077)
    
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            log_warning "Puerto $port está en uso. Esto podría causar conflictos."
        fi
    done
    
    log_success "Verificación de puertos completada"
}

# Construir imágenes Docker
build_images() {
    log_info "Construyendo imágenes Docker..."
    
    log_info "  → Construyendo imagen de Airflow..."
    docker-compose build airflow-webserver
    
    log_info "  → Construyendo imagen de Streamlit..."
    docker-compose build streamlit
    
    log_success "Imágenes Docker construidas"
}

# Iniciar servicios
start_services() {
    log_info "Iniciando servicios..."
    
    log_info "  → Iniciando servicios de infraestructura..."
    docker-compose up -d postgres redis minio
    
    log_info "  → Esperando que PostgreSQL esté listo..."
    sleep 10
    
    log_info "  → Iniciando Spark..."
    docker-compose up -d spark-master spark-worker
    
    log_info "  → Esperando que Spark esté listo..."
    sleep 10
    
    log_info "  → Iniciando Airflow..."
    docker-compose up -d airflow-webserver airflow-scheduler
    
    log_info "  → Esperando que Airflow esté listo..."
    sleep 15
    
    log_info "  → Iniciando Streamlit..."
    docker-compose up -d streamlit
    
    log_success "Todos los servicios iniciados"
}

# Verificar estado de servicios
check_services() {
    log_info "Verificando estado de servicios..."
    
    services=("postgres" "redis" "minio" "spark-master" "spark-worker" "airflow-webserver" "airflow-scheduler" "streamlit")
    
    for service in "${services[@]}"; do
        if docker-compose ps | grep -q "${service}.*Up"; then
            log_success "  ✓ $service está corriendo"
        else
            log_warning "  ⚠ $service no está corriendo correctamente"
        fi
    done
}

# Mostrar información de acceso
show_access_info() {
    echo ""
    echo "🌐 INFORMACIÓN DE ACCESO"
    echo "========================"
    echo ""
    echo -e "${GREEN}Airflow Webserver:${NC}     http://localhost:8081"
    echo -e "${BLUE}  Usuario: admin${NC}"
    echo -e "${BLUE}  Contraseña: admin${NC}"
    echo ""
    echo -e "${GREEN}Spark Master UI:${NC}       http://localhost:8080"
    echo ""
    echo -e "${GREEN}MinIO Console:${NC}         http://localhost:9001"
    echo -e "${BLUE}  Usuario: minioadmin${NC}"
    echo -e "${BLUE}  Contraseña: minioadmin123${NC}"
    echo ""
    echo -e "${GREEN}Streamlit Dashboard:${NC}   http://localhost:8501"
    echo ""
    echo -e "${GREEN}PostgreSQL:${NC}            localhost:5432"
    echo -e "${BLUE}  Base de datos: etl_database${NC}"
    echo -e "${BLUE}  Usuario: etl_user${NC}"
    echo -e "${BLUE}  Contraseña: etl_password${NC}"
}

# Mostrar próximos pasos
show_next_steps() {
    echo ""
    echo "📋 PRÓXIMOS PASOS"
    echo "=================="
    echo ""
    echo "1. Esperar 2-3 minutos para que todos los servicios estén completamente listos"
    echo ""
    echo "2. Configurar conexión PostgreSQL en Airflow:"
    echo "   - Ve a http://localhost:8081"
    echo "   - Login: admin / admin"
    echo "   - Admin → Connections → Create"
    echo "   - Conn Id: postgres_default"
    echo "   - Conn Type: Postgres"
    echo "   - Host: postgres"
    echo "   - Schema: etl_database"
    echo "   - Login: etl_user"
    echo "   - Password: etl_password"
    echo "   - Port: 5432"
    echo ""
    echo "3. Ejecutar DAGs en orden:"
    echo "   a) etl_local_csv_pipeline"
    echo "   b) etl_worldbank_gdp_pipeline"
    echo "   c) ml_model_training_pipeline"
    echo ""
    echo "4. Explorar el dashboard en http://localhost:8501"
    echo ""
    echo "5. Para ver logs: docker-compose logs -f [servicio]"
    echo ""
    echo "6. Para detener todo: docker-compose down"
}

# Función principal
main() {
    echo ""
    log_info "Iniciando configuración del pipeline ETL..."
    echo ""
    
    check_prerequisites
    create_directory_structure
    setup_permissions
    check_config_files
    generate_sample_data
    check_ports
    build_images
    start_services
    
    log_info "Esperando que los servicios se estabilicen..."
    sleep 30
    
    check_services
    show_access_info
    show_next_steps
    
    echo ""
    log_success "🎉 CONFIGURACIÓN COMPLETADA EXITOSAMENTE!"
    echo ""
    echo "El pipeline ETL está listo para usar. Todos los servicios están corriendo."
    echo "Puedes comenzar configurando las conexiones en Airflow y ejecutando los DAGs."
    echo ""
}

# Función para cleanup en caso de error
cleanup_on_error() {
    log_error "Error durante la configuración. Limpiando..."
    docker-compose down 2>/dev/null || true
    exit 1
}

# Trap para manejar errores
trap cleanup_on_error ERR

# Verificar si el script se está ejecutando desde el directorio correcto
if [ ! -f "docker-compose.yml" ] && [ ! -f "setup.sh" ]; then
    log_error "Este script debe ejecutarse desde el directorio raíz del proyecto"
    log_error "Asegúrate de estar en el directorio que contiene docker-compose.yml"
    exit 1
fi

# Verificar argumentos de línea de comandos
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Uso: ./setup.sh [opción]"
        echo ""
        echo "Opciones:"
        echo "  help, -h, --help    Mostrar esta ayuda"
        echo "  clean               Limpiar todos los contenedores y volúmenes"
        echo "  restart             Reiniciar todos los servicios"
        echo "  status              Mostrar estado de los servicios"
        echo "  logs                Mostrar logs de todos los servicios"
        echo "  stop                Detener todos los servicios"
        echo ""
        echo "Sin argumentos: Ejecutar configuración completa"
        exit 0
        ;;
    "clean")
        log_info "Limpiando todos los contenedores y volúmenes..."
        docker-compose down -v
        docker system prune -f
        log_success "Limpieza completada"
        exit 0
        ;;
    "restart")
        log_info "Reiniciando todos los servicios..."
        docker-compose restart
        check_services
        show_access_info
        exit 0
        ;;
    "status")
        log_info "Estado de los servicios:"
        docker-compose ps
        exit 0
        ;;
    "logs")
        log_info "Mostrando logs de todos los servicios..."
        docker-compose logs -f
        exit 0
        ;;
    "stop")
        log_info "Deteniendo todos los servicios..."
        docker-compose down
        log_success "Todos los servicios detenidos"
        exit 0
        ;;
    "")
        # Configuración completa - continuar con main()
        ;;
    *)
        log_error "Opción desconocida: $1"
        log_info "Usa './setup.sh help' para ver las opciones disponibles"
        exit 1
        ;;
esac

# Ejecutar configuración principal
main