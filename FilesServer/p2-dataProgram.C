#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define TAMANO_TABLA 1000003
#define ARCHIVO_DATASET "full_dataset_clean.csv"
#define ARCHIVO_INDICES "index.bin"
#define LIMITE_BUFFER_RESULTADOS 40000
#define PORT 8080 // puerto para la conexión del socket

// estructura para la información de los tweets
struct Tweet {
    long id;
    char fecha[11];   // YYYY-MM-DD
    char tiempo[9];   // HH:MM:SS
    char idioma[4];   // ej: "es", "en"
    char pais[4];     // ej: "DE", "PA"
};

// estructura para recibir los criterios de búsqueda del cliente
struct CriteriosBusqueda {
    char fecha[11];
    char tiempo_inicial[9];
    char tiempo_final[9];
    char idioma[4];
};

// contiene un puntero a un registro en el CSV y un puntero al siguiente nodo de colisión.
struct NodoIndice {
    int64_t offset_csv;
    int64_t offset_siguiente_nodo;
};

// variable global para el descriptor del socket de escucha
int socket_escucha_fd;

// función Hash (utilizando el algoritmo djb2)
unsigned long hash_djb2(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}


// función para crear la tabla hash en disco (indexando el apuntador al registro en el CSV)
void crear_tabla_hash() {
    FILE *dataset_fd, *indice_fd;
    int64_t *tabla_indices;

    printf("Asignando memoria para la tabla de cabeceras del índice (%.2f MB)...\n", (double)TAMANO_TABLA * sizeof(int64_t) / (1024*1024));
    tabla_indices = (int64_t *)malloc(TAMANO_TABLA * sizeof(int64_t));
    if (!tabla_indices) {
        perror("Error al asignar memoria para la tabla de índice");
        exit(-1);
    }
    
    // inicialización todas las cabeceras a -1, indicando listas vacías.
    for (size_t i = 0; i < TAMANO_TABLA; i++) {
        tabla_indices[i] = -1;
    }

    dataset_fd = fopen(ARCHIVO_DATASET, "r");
    if (!dataset_fd) {
        perror("Error al abrir el dataset de entrada");
        free(tabla_indices);
        exit(-1);
    }
    
    indice_fd = fopen(ARCHIVO_INDICES, "wb");
    if (!indice_fd) {
        perror("Error al crear el archivo de índice binario");
        fclose(dataset_fd);
        free(tabla_indices);
        exit(-1);
    }
    
    printf("Escribiendo cabecera de índice en %s...\n", ARCHIVO_INDICES);
    fwrite(tabla_indices, sizeof(int64_t), TAMANO_TABLA, indice_fd);

    printf("Procesando el dataset y construyendo el índice...\n");
    char buffer_linea[256];
    long long contador_tweets = 0;
    
    // guardado del offset después de la cabecera del CSV para empezar a leer datos reales
    int64_t offset_csv_actual = ftell(dataset_fd);

    while (fgets(buffer_linea, sizeof(buffer_linea), dataset_fd)) {
        char cadena_fecha[11] = {0};

        if (sscanf(buffer_linea, "%*[^,],%10[^,]", cadena_fecha) != 1) {
            offset_csv_actual = ftell(dataset_fd);
            continue;
        }

        unsigned long valor_hash = hash_djb2(cadena_fecha);
        unsigned int indice = valor_hash % TAMANO_TABLA;

        struct NodoIndice nuevo_nodo;
        nuevo_nodo.offset_csv = offset_csv_actual;
        nuevo_nodo.offset_siguiente_nodo = tabla_indices[indice];

        fseek(indice_fd, 0, SEEK_END);
        int64_t new_node_offset = ftell(indice_fd);
        fwrite(&nuevo_nodo, sizeof(struct NodoIndice), 1, indice_fd);

        tabla_indices[indice] = new_node_offset;

        contador_tweets++;
        if (contador_tweets % 1000000 == 0) {
            printf("Procesados %lld millones de registros...\n", contador_tweets / 1000000);
        }
        
        offset_csv_actual = ftell(dataset_fd);
    }
    printf("Procesamiento completo. Total de registros indexados: %lld\n", contador_tweets);

    printf("Escribiendo la tabla de cabeceras final en %s...\n", ARCHIVO_INDICES);
    fseek(indice_fd, 0, SEEK_SET);
    fwrite(tabla_indices, sizeof(int64_t), TAMANO_TABLA, indice_fd);
    
    printf("¡Creación del índice finalizada!\n");

    fclose(dataset_fd);
    fclose(indice_fd);
    free(tabla_indices);
}


//escritura de datos para envío por paso de mensajes
ssize_t escribir_datos(int fd, const void *buf, size_t count) {

    size_t bytes_escritos = 0;
    const char *puntero_buffer = (const char *)buf;

    while (bytes_escritos < count) {

        ssize_t res = send(fd, puntero_buffer + bytes_escritos, count - bytes_escritos,0);

        if (res < 0) 
            return res;

        bytes_escritos += res;
    }

    return bytes_escritos;
}

//lectura de datos recibidos mediante paso de mensajes
ssize_t leer_datos(int fd, void *buf, size_t count) {

    size_t bytes_leidos = 0;
    char *puntero_buffer = (char *)buf;

    while (bytes_leidos < count) {

        ssize_t res = recv(fd, puntero_buffer + bytes_leidos, count - bytes_leidos, 0);

        if (res < 0) 
            return res;

        if (res == 0)
            break; // EOF

        bytes_leidos += res;
    }

    return bytes_leidos;
}

// función para buscar coincidencias en el CSV usando la tabla hash 
void busqueda(int socket_cliente, const struct CriteriosBusqueda *criterios) {
    
    FILE *indice_fd, *dataset_fd;
    int64_t *index_table;

    index_table = (int64_t *)malloc(TAMANO_TABLA * sizeof(int64_t));
    if (!index_table) {
        perror("Error al asignar memoria para la tabla de índice");
        exit(-1);
    }

    indice_fd = fopen(ARCHIVO_INDICES, "rb");
    if (!indice_fd) {
        perror("No se pudo abrir el archivo de índice");
        free(index_table);
        exit(-1);
    }

    fread(index_table, sizeof(int64_t), TAMANO_TABLA, indice_fd);

    dataset_fd = fopen(ARCHIVO_DATASET, "r");
    if (!dataset_fd) {
        perror("No se pudo abrir el archivo de datos CSV");
        fclose(indice_fd);
        free(index_table);
        exit(-1);
    }

    unsigned long valor_hash = hash_djb2(criterios->fecha);
    unsigned int indice = valor_hash % TAMANO_TABLA;
    int64_t offset_nodo_actual = index_table[indice];

    int cantidad_coincidencias = 0;
    struct Tweet buffer_resultados[LIMITE_BUFFER_RESULTADOS];

    if (offset_nodo_actual != -1) {
        struct NodoIndice nodo_actual;
        char buffer_linea[256];

        while (offset_nodo_actual != -1 && cantidad_coincidencias < LIMITE_BUFFER_RESULTADOS) {
            fseek(indice_fd, offset_nodo_actual, SEEK_SET);
            fread(&nodo_actual, sizeof(struct NodoIndice), 1, indice_fd);

            fseek(dataset_fd, nodo_actual.offset_csv, SEEK_SET);
            fgets(buffer_linea, sizeof(buffer_linea), dataset_fd);

            struct Tweet tweet = {0};
            char *token_dataset;
            char *puntero_guardado;
            buffer_linea[strcspn(buffer_linea, "\n")] = 0;

            token_dataset = strtok_r(buffer_linea, ",", &puntero_guardado);
            if (token_dataset) tweet.id = atoll(token_dataset);
            token_dataset = strtok_r(NULL, ",", &puntero_guardado);
            if (token_dataset) strncpy(tweet.fecha, token_dataset, sizeof(tweet.fecha) - 1);
            token_dataset = strtok_r(NULL, ",", &puntero_guardado);
            if (token_dataset) strncpy(tweet.tiempo, token_dataset, sizeof(tweet.tiempo) - 1);
            token_dataset = strtok_r(NULL, ",", &puntero_guardado);
            if (token_dataset) strncpy(tweet.idioma, token_dataset, sizeof(tweet.idioma) - 1);
            token_dataset = strtok_r(NULL, "\n\r", &puntero_guardado);
            if (token_dataset) strncpy(tweet.pais, token_dataset, sizeof(tweet.pais) - 1);

            if (strcmp(tweet.fecha, criterios->fecha) == 0 &&
                strcmp(tweet.idioma, criterios->idioma) == 0 &&
                strcmp(tweet.tiempo, criterios->tiempo_inicial) >= 0 &&
                strcmp(tweet.tiempo, criterios->tiempo_final) <= 0)
            {
                buffer_resultados[cantidad_coincidencias] = tweet;
                cantidad_coincidencias++;
            }
            
            offset_nodo_actual = nodo_actual.offset_siguiente_nodo;
        }
    }

    //enviar la cantidad de resultados al cliente
    printf("Enviando %d resultados al cliente.\n", cantidad_coincidencias);
    if (escribir_datos(socket_cliente, &cantidad_coincidencias, sizeof(int)) != sizeof(int)) {
        perror("Error al enviar la cantidad de resultados");
    }
   

    // si hay resultados, enviarlos
    if (cantidad_coincidencias > 0) {
       size_t bytes_a_enviar = sizeof(struct Tweet) * cantidad_coincidencias;
       if (escribir_datos(socket_cliente, buffer_resultados, bytes_a_enviar) != bytes_a_enviar) {
            perror("Error al enviar los datos de los tweets");
        }
    }

    fclose(dataset_fd);
    fclose(indice_fd);
    free(index_table);
}


int existe_archivo(const char *path) {
    return access(path, F_OK) == 0;
}

void cerrar_servidor(int sig) {
    printf("\nCerrando el servidor...\n");
    close(socket_escucha_fd);
    exit(0);
}

void manejar_cliente(int socket_cliente) {
    printf("Cliente conectado. Esperando criterios de búsqueda...\n");
    
    struct CriteriosBusqueda criterios;
    
     
    ssize_t bytes_leidos = leer_datos(socket_cliente, &criterios, sizeof(struct CriteriosBusqueda));
    
    if (bytes_leidos <= 0) {
        if (bytes_leidos == 0) {
            printf("El cliente cerró la conexión.\n");
        } else {
            perror("Error al leer del socket del cliente");
        }
        close(socket_cliente);
        exit(-1);
    }
    
    printf("Búsqueda solicitada para fecha: %s, idioma: %s\n", criterios.fecha, criterios.idioma);

    // Realizar la búsqueda y enviar los resultados
    busqueda(socket_cliente, &criterios);

    printf("Búsqueda completada. Cerrando conexión con el cliente.\n");
    close(socket_cliente);
}

int main() {

    // se bloquea la señal de salida SIGINT (ocurre cuando se oprime ctrl+C)
    signal(SIGINT, cerrar_servidor);

    // crear el índice si no existe
    if (!existe_archivo(ARCHIVO_INDICES)) {
        printf("Archivo de índice no encontrado. Creándolo ahora...\n");
        crear_tabla_hash();
    } else {
        printf("Archivo de índice encontrado.\n");
    }

    int socket_cliente_fd;
    struct sockaddr_in direccion_servidor, direccion_cliente;
    socklen_t longitud_cliente;

    // crear socket
    socket_escucha_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_escucha_fd == -1) {
        perror("Error al crear el socket");
        exit(-1);
    }

    // permitir la reutilización de la dirección/puerto
    int opt = 1;
    if (setsockopt(socket_escucha_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("Error en setsockopt");
        close(socket_escucha_fd);
        exit(-1);
    }

    // configurar la dirección del servidor
    direccion_servidor.sin_family = AF_INET;
    direccion_servidor.sin_addr.s_addr = INADDR_ANY; // Escuchar en todas las interfaces de red
    direccion_servidor.sin_port = htons(PORT);
    bzero(direccion_servidor.sin_zero,6);

    // enlazar el socket al puerto
    if (bind(socket_escucha_fd, (struct sockaddr *)&direccion_servidor, sizeof(direccion_servidor)) < 0) {
        perror("Error al enlazar el socket");
        close(socket_escucha_fd);
        exit(-1);
    }

    // poner el socket en modo de escucha
    if (listen(socket_escucha_fd, 5) < 0) { // 5 es el backlog de conexiones pendientes
        perror("Error al poner el socket en modo de escucha");
        close(socket_escucha_fd);
        exit(-1);
    }

    printf("Servidor escuchando en el puerto %d\n", PORT);
    printf("Esperando conexiones de clientes...\n");

    // bucle principal para aceptar clientes
    while (1) {
        longitud_cliente = sizeof(direccion_cliente);
        socket_cliente_fd = accept(socket_escucha_fd, (struct sockaddr *)&direccion_cliente, &longitud_cliente);
        if (socket_cliente_fd < 0) {
            perror("Error al aceptar la conexión");
            continue; 
        }

        manejar_cliente(socket_cliente_fd);
    }

    close(socket_escucha_fd);
    return 0;
}