#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// opciones del menu
#define INGRESAR_PRIMER_CRITERIO 1
#define INGRESAR_SEGUNDO_CRITERIO 2
#define INGRESAR_TERCER_CRITERIO 3
#define BUSCAR 4
#define VER_CRITERIOS 5
#define SALIR 6

// configuracion de Red
//#define IP_SERVIDOR "127.0.0.1" 
#define IP_SERVIDOR "34.151.203.146" 
#define PORT 8080

// medidas de ancho para la impresion de resultados
#define ANCHO_ID 20
#define ANCHO_FECHA 12
#define ANCHO_TIEMPO 10
#define ANCHO_IDIOMA 8
#define ANCHO_PAIS 8

// estructuras
struct Tweet {
    long id;
    char fecha[11];
    char tiempo[9];
    char idioma[4];
    char pais[4];
};

struct CriteriosBusqueda {
    char fecha[11];
    char tiempo_inicial[9];
    char tiempo_final[9];
    char idioma[4];
};

// cirterios ingresados por el usuario para la busqueda de tweets
char criterio_fecha[11], criterio_idioma[4], criterio_tiempo_inicio[9], criterio_tiempo_final[9];

void mostrar_menu() {
    printf("\n1. Ingresar primer criterio de busqueda (fecha).\n");
    printf("2. Ingresar segundo criterio de busqueda (rango de tiempo).\n");
    printf("3. Ingresar tercer criterio de busqueda (idioma).\n");
    printf("4. Realizar busqueda.\n");
    printf("5. Ver criterios.\n");
    printf("6. Salir.\n");
}

// función para limpiar posibles caracteres que se hallan quedado en el buffer
void limpiar_buffer() {
    int caracter;
    while ((caracter = getchar()) != '\n' && caracter != EOF);
}

bool es_bisiesto(int año) {
    return (año % 4 == 0 && año % 100 != 0) || (año % 400 == 0);
}

// función para saber los dias de un mes, segun el mes y el año
int dias_del_mes(int mes, int año) {
    if (mes == 2)
        return es_bisiesto(año) ? 29 : 28;
    if (mes == 4 || mes == 6 || mes == 9 || mes == 11)
        return 30;

    return 31;
}

// función para ingresar la fecha
void ingresar_primer_criterio() {
    
    limpiar_buffer();

    int dia, mes, año;
    char linea[100]; // buffer para leer la línea completa
    char caracter_extra; // variable para verificar si hay caracteres extra después del número
    int resultado_del_escaneo;

    // obtención del año actual
    time_t ahora = time(NULL);
    struct tm *info_tiempo = localtime(&ahora);
    int año_actual = info_tiempo->tm_year + 1900;

    // ingresar año
    do {
        printf("Por favor, ingrese el año (entre 2019 y %d):\n", año_actual);
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &año, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero. (Ej: 2019, no 2019k)\n");
                año = -1;
            }


        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            año = -1;
        }

        if (año != -1 && (año < 2019 || año > año_actual)) {
            printf("El año %d no es válido. Debe estar entre 2019 y %d.\n", año, año_actual);
            año = -1;
        }
    } while (año == -1);

    // ingresar mes
    do {
        printf("Por favor, ingrese el mes (entre 1 y 12):\n");
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &mes, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero.\n");
                mes = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            mes = -1;
        }

        if (mes != -1 && (mes < 1 || mes > 12)) {
            printf("El mes %d no es válido. Debe estar entre 1 y 12.\n", mes);
            mes = -1;
        }
    } while (mes == -1);

    // Ingresar día
    int dias_mes = dias_del_mes(mes, año);
    do {
        printf("Por favor, ingrese el día (entre 1 y %d):\n", dias_mes);
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &dia, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero.\n");
                dia = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            dia = -1;
        }

        if (dia != -1 && (dia < 1 || dia > dias_mes)) {
            printf("El día %d no es válido. Debe estar entre 1 y %d.\n", dia, dias_mes);
            dia = -1;
        }
    } while (dia == -1);

    sprintf(criterio_fecha, "%04d-%02d-%02d", año, mes, dia);
}

// funcion para ingresar la hora y el minuto
void ingresar_segundo_criterio() {
    
    limpiar_buffer();

    int hora_inicial, minuto_inicial;
    char linea[100]; // buffer para leer la línea completa
    char caracter_extra; // variable para verificar si hay caracteres extra después del número
    int resultado_del_escaneo;

    // ingresar hora inicial
    do {
        printf("Por favor, ingrese la hora inicial (entre 0 y 23)\n");
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &hora_inicial, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero. (Ej: 12, no 12k)\n");
                hora_inicial = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            hora_inicial = -1;
        }

        if (hora_inicial != -1 && (hora_inicial < 0 || hora_inicial > 23)) {
            printf("La hora inicial %d no es valida. Debe estar entre 0 y 23.\n", hora_inicial);
            hora_inicial = -1;
        }
    } while (hora_inicial == -1);

    // ingresar minuto inicial
    do {
        printf("Por favor, ingrese el minuto inicial (entre 0 y 59)\n");
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &minuto_inicial, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero. (Ej: 12, no 12k)\n");
                minuto_inicial = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            minuto_inicial = -1;
        }

        if (minuto_inicial != -1 && (minuto_inicial < 0 || minuto_inicial > 59)) {
            printf("EL minuto inicial %d no es valido. Debe estar entre 0 y 59.\n", minuto_inicial);
            minuto_inicial = -1;
        }
    } while (minuto_inicial == -1);

    sprintf(criterio_tiempo_inicio, "%02d:%02d:00", hora_inicial, minuto_inicial);

    sprintf(criterio_tiempo_final, "%02d:%02d:59", hora_inicial, minuto_inicial);


    printf("\nTiempo inicial establecido en: %s\n", criterio_tiempo_inicio);
    printf("Tiempo final establecido en: %s\n", criterio_tiempo_final);
    
}

// funcion para ingresar el idioma
void ingresar_tercer_criterio() {
    
    char idioma_temporal[4] = {0}; //variable para almacenar los caracteres que ingrese el usuario
    char caracter;
        
    limpiar_buffer();
    
    while (true) {
        printf("Por favor, ingrese el código de idioma (2-3 letras): ");
                
        // se leen exactamente 3 caracteres del buffer
        int cantidad_de_caracteres = 0;
        while (true) {
            caracter = getchar();
            if (caracter == '\n' || caracter == EOF) {
                if(cantidad_de_caracteres < 4)
                    idioma_temporal[cantidad_de_caracteres] = '\0';
                break;
            }
            
            if(cantidad_de_caracteres < 4){
                idioma_temporal[cantidad_de_caracteres] = caracter;
            }

            cantidad_de_caracteres++;
        }
        
        // se valida la cantidad de caracteres ingresados
        if (cantidad_de_caracteres < 2 || cantidad_de_caracteres > 3) {
            printf("Debe ingresar exactamente 2 o 3 letras.\n");
            continue;
        }
        
        // se valida que solo se hayan ingresado caracteres alfabeticos
        bool es_valido = true;
        for (int indice_caracter_actual = 0; indice_caracter_actual < cantidad_de_caracteres; indice_caracter_actual++) {
            if (!isalpha((unsigned char)idioma_temporal[indice_caracter_actual])) {
                es_valido = false;
                break;
            }
        }
        
        if (!es_valido) {
            printf("Solo se permiten letras (a-z, A-Z).\n");
            continue;
        }
        
        // se convierte todos los caracteres a minuscula
        for (int indice_caracter_actual = 0; indice_caracter_actual < cantidad_de_caracteres; indice_caracter_actual++) {
            idioma_temporal[indice_caracter_actual] = tolower((unsigned char)idioma_temporal[indice_caracter_actual]);
        }
        
        strcpy(criterio_idioma, idioma_temporal);
        break;
    }
}

void ver_criterios(){
    
    // se observa el primer caracter para saber si un criterio no ha sido inicialiad
    
    printf("\n--- Criterios de Búsqueda Actuales ---\n");
    if(criterio_fecha[0] == '\0'){
        printf("\nNo se ha establecido una fecha.\n");
    } else {
        printf("\nFecha: %s\n", criterio_fecha);
    }

    if(criterio_tiempo_inicio[0] == '\0'){
        printf("No se ha establecido un tiempo inicial.\n");
    } else {
        printf("Tiempo inicial: %s\n", criterio_tiempo_inicio);
    }

    if(criterio_tiempo_final[0] == '\0'){
        printf("No se ha establecido un tiempo final.\n");
    } else {
        printf("Tiempo final: %s\n", criterio_tiempo_final);
    }

    if(criterio_idioma[0] == '\0'){
        printf("No se ha establecido un idioma.\n");
    } else {
        printf("Idioma: %s\n", criterio_idioma);
    }
    printf("--------------------------------------\n");
}


ssize_t escribir_datos(int fd, const void *buffer, size_t count) {

    size_t bytes_escritos = 0;
    const char *puntero_buffer = (const char *)buffer;

    while (bytes_escritos < count) {

        ssize_t res = send(fd, puntero_buffer + bytes_escritos, count - bytes_escritos, 0);

        if (res <= 0)
            return res;

        bytes_escritos += res;

    }

    return bytes_escritos;
}

ssize_t leer_datos(int fd, void *buffer, size_t count) {

    size_t bytes_leidos = 0;
    char *puntero_buffer = (char *)buffer;

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

void buscar() {

    int socket_fd;
    struct sockaddr_in direccion_servidor;
    struct CriteriosBusqueda criterios;
    int cantidad_resultados;
    struct timespec inicio, final;

    // creacion del socket
    if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error al crear el socket");
        exit(-1);
    }

    // configuración de la dirección del servidor
    direccion_servidor.sin_family = AF_INET;
    direccion_servidor.sin_port = htons(PORT);
    direccion_servidor.sin_addr.s_addr = inet_addr(IP_SERVIDOR);
    bzero(direccion_servidor.sin_zero, 8);


    // conexión al servidor
    if (connect(socket_fd, (struct sockaddr *)&direccion_servidor, sizeof(direccion_servidor)) < 0) {
        perror("Error al conectar con el servidor");
        close(socket_fd);
        exit(-1);
    }

    // Preparación de los criterios de búsqueda
    strcpy(criterios.fecha, criterio_fecha);
    strcpy(criterios.tiempo_inicial, criterio_tiempo_inicio);
    strcpy(criterios.tiempo_final, criterio_tiempo_final);
    strcpy(criterios.idioma, criterio_idioma);

    if (escribir_datos(socket_fd, &criterios, sizeof(criterios)) != sizeof(criterios)) {
        perror("Error al enviar los criterios de búsqueda");
        close(socket_fd);
        exit(-1);
    }

    clock_gettime(CLOCK_MONOTONIC, &inicio);
    
    // Leer la cantidad de resultados
    if (leer_datos(socket_fd, &cantidad_resultados, sizeof(cantidad_resultados)) != sizeof(cantidad_resultados)) {
        perror("Error al recibir la cantidad de resultados");
        close(socket_fd);
        exit(-1);
    }
    
    if (cantidad_resultados > 0) {
        
        struct Tweet *resultados = (struct Tweet *)malloc(sizeof(struct Tweet) * cantidad_resultados);

        if (!resultados) {
            perror("Error al asignar memoria para los resultados");
            close(socket_fd);
            exit(-1);
        }

        size_t bytes_a_recibir = sizeof(struct Tweet) * cantidad_resultados;

        if (leer_datos(socket_fd, resultados, bytes_a_recibir) != (long int) bytes_a_recibir) {
             perror("Error al recibir los datos de los tweets");
             free(resultados);
             close(socket_fd);
             exit(-1);
        }

        // impresión de resulados con formato tabular
        printf("\n+");
        for (int i = 0; i < ANCHO_ID + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_FECHA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_TIEMPO + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_IDIOMA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_PAIS + 2; i++) printf("-");
        printf("+\n");

        printf("| %-*s | %-*s | %-*s | %-*s | %-*s |\n",
               ANCHO_ID, "ID",
               ANCHO_FECHA, "Fecha",
               ANCHO_TIEMPO, "Tiempo",
               ANCHO_IDIOMA, "Idioma",
               ANCHO_PAIS, "Pais");

        printf("+");
        for (int i = 0; i < ANCHO_ID + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_FECHA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_TIEMPO + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_IDIOMA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_PAIS + 2; i++) printf("-");
        printf("+\n");
        for (int i = 0; i < cantidad_resultados; i++) {
            if (strlen(resultados[i].pais) == 0) {
                strcpy(resultados[i].pais, "-");
            }
            printf("| %-*ld | %-*s | %-*s | %-*s | %-*s |\n",
                ANCHO_ID, resultados[i].id,
                ANCHO_FECHA, resultados[i].fecha,
                ANCHO_TIEMPO, resultados[i].tiempo,
                ANCHO_IDIOMA, resultados[i].idioma,
                ANCHO_PAIS, resultados[i].pais);
        }
        printf("+");
        for (int i = 0; i < ANCHO_ID + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_FECHA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_TIEMPO + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_IDIOMA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_PAIS + 2; i++) printf("-");
        printf("+\n");

        free(resultados);

        clock_gettime(CLOCK_MONOTONIC, &final);
        double tiempo_transcurrido = (final.tv_sec - inicio.tv_sec) + (final.tv_nsec - inicio.tv_nsec) / 1e9;
        printf("Se han encontrado %d resultados en %.4f segundos.\n", cantidad_resultados, tiempo_transcurrido);

    } else {
        printf("\nNA\n");
    }

    close(socket_fd);
}

void salir() {
    printf("Saliendo...\n");
    exit(0);
}

void señal_de_cierre(int sig) {
    printf("\nOperación interrumpida. Saliendo...\n");
    exit(0);
}

int main() {

    bool informacion_faltante = false;
    int opcion;

    signal(SIGINT, señal_de_cierre); // Capturar Ctrl+C

    printf("\nBienvenido al Cliente de Búsqueda de Tweets\n");

    while (true) {
        mostrar_menu();
        printf("Seleccione una opción: ");

        if (scanf("%d", &opcion) != 1) {
            printf("Entrada inválida. Por favor, ingrese un número.\n");
            limpiar_buffer();
            continue;
        }

        switch (opcion) {
            case INGRESAR_PRIMER_CRITERIO:
                ingresar_primer_criterio();
                break;
            case INGRESAR_SEGUNDO_CRITERIO:
                ingresar_segundo_criterio();
                break;
            case INGRESAR_TERCER_CRITERIO:
                ingresar_tercer_criterio();
                break;
            case BUSCAR:

                printf("\n");

                // se verifica que todos los criterios hayan sido indicados antes de buscar
                informacion_faltante = false;

                if(criterio_fecha[0] == '\0'){
                    printf("Para realizar la busqueda, debe especificar una fecha.\n");
                    informacion_faltante = true;
                }

                if(criterio_tiempo_inicio[0] == '\0'){
                    printf("Para realizar la busqueda, debe especificar un tiempo inicial.\n");
                    informacion_faltante = true;
                }

                if(criterio_tiempo_final[0] == '\0'){
                    printf("Para realizar la busqueda, debe especificar un tiempo final.\n");
                    informacion_faltante = true;
                }

                if(criterio_idioma[0] == '\0'){
                    printf("Para realizar la busqueda, debe especificar un idioma.\n");
                    informacion_faltante = true;
                }

                if(informacion_faltante){
                    continue;
                }

                buscar();

                break;  
            case VER_CRITERIOS:
                ver_criterios();
                break;
            case SALIR:
                salir();
                break;
            default:
                printf("Opción inválida. Inténtelo de nuevo.\n");
                break;
        }
    }

    return 0;
}