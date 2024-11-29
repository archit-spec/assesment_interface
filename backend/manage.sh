#!/bin/bash

# Function to display usage
show_usage() {
    echo "Usage: $0 {start|stop|restart|logs|status}"
    echo "Commands:"
    echo "  start   - Start the services"
    echo "  stop    - Stop the services"
    echo "  restart - Restart the services"
    echo "  logs    - Show logs"
    echo "  status  - Check service status"
}

# Create required directories
create_directories() {
    mkdir -p uploads processed logs
}

case "$1" in
    start)
        echo "Starting services..."
        create_directories
        docker-compose up -d
        echo "Services started"
        ;;
    stop)
        echo "Stopping services..."
        docker-compose down
        echo "Services stopped"
        ;;
    restart)
        echo "Restarting services..."
        docker-compose restart
        echo "Services restarted"
        ;;
    logs)
        docker-compose logs -f
        ;;
    status)
        docker-compose ps
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
