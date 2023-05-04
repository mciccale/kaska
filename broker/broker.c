#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <dirent.h>

#include "comun.h"
#include "map.h"
#include "queue.h"

// información que se la pasa el thread creado
typedef struct thread_info
{
    int socket; // añadir los campos necesarios
} thread_info;

// mapa que guarda todos los descriptores de temas
map *topics;
map_iter *it;
map_position *pos;

// magic number
const char *magic_number = "KASK";

// variable que almacena el nombre del directorio
char *directorio_commits;
char *directorio_data;

// variable para descartar los datos
char trash[BUFFER_SIZE];

// inicializa el socket y lo prepara para aceptar conexiones
static int init_socket_server(const char *port)
{
    int s;
    struct sockaddr_in dir;
    int opcion = 1;

    // socket stream para Internet: TCP
    if ((s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    {
        perror("error creando socket");
        return -1;
    }

    // Para reutilizar puerto inmediatamente si se rearranca el servidor
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion)) < 0)
    {
        perror("error en setsockopt");
        return -1;
    }

    // asocia el socket al puerto especificado
    dir.sin_addr.s_addr = INADDR_ANY;
    dir.sin_port = htons(atoi(port));
    dir.sin_family = PF_INET;
    if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0)
    {
        perror("error en bind");
        close(s);
        return -1;
    }

    // establece el nº máx. de conexiones pendientes de aceptar
    if (listen(s, 5) < 0)
    {
        perror("error en listen");
        close(s);
        return -1;
    }

    return s;
}

// función del thread
void *servicio(void *arg)
{
    int cod_op, res, entero, longitud_str, longitud_msg, longitud_cliente;
    char *cliente, *string, *ruta_subdirectorio, *ruta_fichero;
    message *msg_desc;
    topic_t *topic;
    thread_info *thinf = arg;

    while (1)
    {
        // se recibe el código de operación
        if (recv(thinf->socket, &cod_op, sizeof(int), MSG_WAITALL) != sizeof(int))
            break;
        cod_op = ntohl(cod_op);
        printf("Recibido código de operación: %d\n", cod_op);

        // switch para gestionar los diferentes tipos de "peticiones"
        switch (cod_op)
        {
        case CREATE_TOPIC:
        {
            topic = malloc(sizeof(topic_t));
            // se recibe la longitud del string
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            topic->nombre = malloc(longitud_str + 1);

            // se recibe el string
            if (recv(thinf->socket, topic->nombre, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
            }
            topic->nombre[longitud_str] = '\0';
            printf("Recibido el tema a crear: %s\n", topic->nombre);

            // se crea la estructura del tema
            topic->cola = queue_create(0);

            // se abre el directorio donde se tiene que crear el archivo de persistencia
            int dir_fd = open(directorio_data, O_DIRECTORY);
            if (dir_fd < 0)
            {
                printf("No se ha podido abrir el directorio\n");
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
            }

            // se tiene que crear el fichero con el nombre del tema en la proyeccion
            // el formato del archivo sera: KASK[longitud_msg][msg]...[longitud_msg][msg]
            int fd = openat(dir_fd, topic->nombre, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            if (fd < 0)
            {
                printf("No se ha podido crear o abrir el fichero\n");
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
            }

            // se establece el tamanyo
            ftruncate(fd, FILE_SIZE);

            // se proyecta el fichero con mmap
            topic->buf = mmap(NULL, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (topic->buf == NULL)
            {
                printf("Fallo en la proyeccion del fichero\n");
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
            }

            // se escribe el magic number
            memcpy(topic->buf, magic_number, strlen(magic_number));

            // se avanza el puntero para apuntar a la siguiente posición libre
            topic->buf += strlen(magic_number);

            if (map_put(topics, topic->nombre, topic) < 0)
                res = htonl(-1);
            else
                res = htonl(0);

            // se envía el código de respuesta
            write(thinf->socket, &res, sizeof(int));
            break;
        }
        case NTOPICS:
        {
            // se envía la cantidad de temas
            res = htonl(map_size(topics));
            write(thinf->socket, &res, sizeof(int));
            break;
        }
        case SEND_MSG:
        {
            // se recibe la longitud del tema
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            string = malloc(longitud_str + 1);

            // se recibe el tema
            if (recv(thinf->socket, string, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
                break;
            }
            string[longitud_str] = '\0';
            printf("Recibido tema: %s\n", string);

            // se comprueba si el tema existe
            topic = map_get(topics, string, &res);
            if (res < 0)
            {
                printf("No existe el tema recibido: %s\n", string);
                // como todavía quedan datos en el socket, tenemos que descartarlos
                while (1)
                {
                    if (recv(thinf->socket, trash, BUFFER_SIZE, 0) < BUFFER_SIZE)
                        break;
                }
                res = htonl(res);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se crea el descriptor del mensaje
            msg_desc = malloc(sizeof(message));

            // se recibe la longitud del mensaje
            if (recv(thinf->socket, &longitud_msg, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_msg = ntohl(longitud_msg);
            printf("Recibida longitud del mensaje: %d\n", longitud_msg);

            // se copia el tamanyo en el archivo
            memcpy(topic->buf, &longitud_msg, sizeof(int));
            // se avanza el puntero
            topic->buf = (char *)topic->buf + sizeof(int);

            // se recibe el mensaje
            if (recv(thinf->socket, topic->buf, longitud_msg, MSG_WAITALL) != longitud_msg)
            {
                break;
                break;
            }

            // se copia la referencia al inicio del mensaje en el descriptor
            msg_desc->content = topic->buf;
            msg_desc->length = longitud_msg;

            // se avanza el puntero
            topic->buf = (char *)topic->buf + longitud_msg;
            printf("Recibido msg\n");

            // se encola
            entero = queue_append(topic->cola, msg_desc);
            // se envía el offset
            entero = htonl(entero);
            write(thinf->socket, &entero, sizeof(int));
            break;
        }
        case MSG_LENGTH:
        {
            // se recibe la longitud del tema
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            string = malloc(longitud_str + 1);

            // se recibe el tema
            if (recv(thinf->socket, string, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
                break;
            }
            string[longitud_str] = '\0';
            printf("Recibido tema: %s\n", string);

            // se recibe el offset correspondiente
            if (recv(thinf->socket, &entero, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            entero = ntohl(entero);
            printf("Recibido el offset: %d\n", entero);

            // se comprueba si el tema existe
            topic = map_get(topics, string, &res);
            if (res < 0)
            {
                printf("No existe el tema recibido: %s\n", string);
                res = htonl(res);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se obtiene el mensaje en el offset
            msg_desc = queue_get(topic->cola, entero, &res);
            if (res < 0)
            {
                printf("No existe un mensaje en el offset: %d\n", entero);
                res = htonl(0);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se envía el tamaño en bytes del mensaje
            res = htonl(msg_desc->length);
            write(thinf->socket, &res, sizeof(int));
            break;
        }
        case END_OFFSET:
        {
            // se recibe la longitud del tema
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            string = malloc(longitud_str + 1);

            // se recibe el tema
            if (recv(thinf->socket, string, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
                break;
            }
            string[longitud_str] = '\0';
            printf("Recibido tema: %s\n", string);

            // se comprueba si el tema existe
            topic = map_get(topics, string, &res);
            if (res < 0)
            {
                printf("No existe el tema recibido: %s\n", string);
                res = htonl(res);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se envía el offset del servidor en dicho tema
            res = htonl(queue_size(topic->cola));
            write(thinf->socket, &res, sizeof(int));
            break;
        }
        case POLL:
        {
            // se recibe la longitud del tema
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            string = malloc(longitud_str + 1);

            // se recibe el tema
            if (recv(thinf->socket, string, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
                break;
            }
            string[longitud_str] = '\0';
            printf("Recibido tema: %s\n", string);

            // se recibe el offset correspondiente
            if (recv(thinf->socket, &entero, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            entero = ntohl(entero);
            printf("Recibido el offset: %d\n", entero);

            struct iovec iov[2]; // hay que enviar 2 elementos

            // se comprueba si el tema existe
            topic = map_get(topics, string, &res);
            if (res < 0)
            {
                printf("No existe el tema recibido: %s\n", string);
                res = htonl(-1);
                iov[0].iov_base = &res;
                iov[0].iov_len = sizeof(int);
                iov[1].iov_base = &res;
                iov[1].iov_len = sizeof(int);
                writev(thinf->socket, iov, 2);
                break;
                break;
            }

            // se obtiene el mensaje en el offset
            msg_desc = queue_get(topic->cola, entero, &res);
            if (res < 0)
            {
                printf("No existe un mensaje en el offset: %d\n", entero);
                res = htonl(0);
                iov[0].iov_base = &res;
                iov[0].iov_len = sizeof(int);
                iov[1].iov_base = &res;
                iov[1].iov_len = sizeof(int);
                writev(thinf->socket, iov, 2);
                break;
                break;
            }

            // se envían el tamaño del mensaje y el mensaje
            int longitud_net = htonl(msg_desc->length);
            iov[0].iov_base = &longitud_net;
            iov[0].iov_len = sizeof(int);
            iov[1].iov_base = msg_desc->content;
            iov[1].iov_len = msg_desc->length;
            writev(thinf->socket, iov, 2);
            break;
        }
        case COMMIT:
        {
            // se recibe la longitud del cliente
            if (recv(thinf->socket, &longitud_cliente, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_cliente = ntohl(longitud_cliente);
            printf("Recibida longitud del cliente: %d\n", longitud_cliente);
            cliente = malloc(longitud_cliente + 1);

            // se recibe el cliente
            if (recv(thinf->socket, cliente, longitud_cliente, MSG_WAITALL) != longitud_cliente)
            {
                break;
                break;
            }
            cliente[longitud_cliente] = '\0';
            printf("Recibido cliente: %s\n", cliente);

            // se recibe la longitud del tema
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            string = malloc(longitud_str + 1);

            // se recibe el tema
            if (recv(thinf->socket, string, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
                break;
            }
            string[longitud_str] = '\0';
            printf("Recibido tema: %s\n", string);

            // se recibe el offset correspondiente
            if (recv(thinf->socket, &entero, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            entero = ntohl(entero);
            printf("Recibido el offset: %d\n", entero);

            // hay que comprobar que el tema existe
            topic = map_get(topics, string, &res);
            if (res < 0)
            {
                printf("No existe el tema recibido: %s\n", string);
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se crea la ruta del subdirectorio
            ruta_subdirectorio = malloc(strlen(directorio_commits) + 1 + longitud_cliente + 1);
            sprintf(ruta_subdirectorio, "%s/%s", directorio_commits, cliente);

            // se crea en caso de que no exista
            DIR *subdir = opendir(ruta_subdirectorio);
            if (subdir == NULL)
            {
                if (mkdir(ruta_subdirectorio, 0700) != 0)
                {
                    printf("No se ha podido crear el subdirectorio\n");
                    res = htonl(-1);
                    write(thinf->socket, &res, sizeof(int));
                    break;
                    break;
                }
            }

            // se crea el fichero con el nombre del tema dentro del subdirectorio del cliente
            ruta_fichero = malloc(strlen(ruta_subdirectorio) + 1 + longitud_str + 1);
            sprintf(ruta_fichero, "%s/%s", ruta_subdirectorio, string);

            FILE *fp = fopen(ruta_fichero, "w");
            if (fp == NULL)
            {
                printf("No se ha podido crear o abrir el fichero\n");
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se escribe el offset en el fichero
            fprintf(fp, "%d", entero);
            fclose(fp);

            // se envía OK
            res = htonl(0);
            write(thinf->socket, &res, sizeof(int));
            break;
        }
        case COMMITED:
        {
            // se recibe la longitud del cliente
            if (recv(thinf->socket, &longitud_cliente, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_cliente = ntohl(longitud_cliente);
            printf("Recibida longitud del cliente: %d\n", longitud_cliente);
            cliente = malloc(longitud_cliente + 1);

            // se recibe el cliente
            if (recv(thinf->socket, cliente, longitud_cliente, MSG_WAITALL) != longitud_cliente)
            {
                break;
                break;
            }
            cliente[longitud_cliente] = '\0';
            printf("Recibido cliente: %s\n", cliente);

            // se recibe la longitud del tema
            if (recv(thinf->socket, &longitud_str, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                break;
                break;
            }
            longitud_str = ntohl(longitud_str);
            printf("Recibida longitud del tema: %d\n", longitud_str);
            string = malloc(longitud_str + 1);

            // se recibe el tema
            if (recv(thinf->socket, string, longitud_str, MSG_WAITALL) != longitud_str)
            {
                break;
                break;
            }
            string[longitud_str] = '\0';
            printf("Recibido tema: %s\n", string);

            // hay que comprobar que el tema existe
            topic = map_get(topics, string, &res);
            printf("Se comprueba si el tema existe");
            if (res < 0)
            {
                printf("No existe el tema recibido: %s\n", string);
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se crea la ruta que deberia tener el subdirectorio
            ruta_subdirectorio = malloc(strlen(directorio_commits) + 1 + longitud_cliente + 1);
            sprintf(ruta_subdirectorio, "%s/%s", directorio_commits, cliente);

            // se comprueba que existe el subdirectorio
            DIR *subdir = opendir(ruta_subdirectorio);
            if (subdir == NULL)
            {
                printf("No existe un subdirectorio asociado al cliente: %s\n", cliente);
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se crea la ruta que deberia tener el archivo asociado al tema
            ruta_fichero = malloc(strlen(ruta_subdirectorio) + 1 + longitud_str + 1);
            sprintf(ruta_fichero, "%s/%s", ruta_subdirectorio, string);

            // se comprueba que existe el fichero
            FILE *fp = fopen(ruta_fichero, "r");
            if (fp == NULL)
            {
                printf("No se ha podido abrir el fichero\n");
                res = htonl(-1);
                write(thinf->socket, &res, sizeof(int));
                break;
                break;
            }

            // se obtiene el offset del fichero
            fscanf(fp, "%d", &entero);
            fclose(fp);

            // se envía el offset
            res = htonl(entero);
            write(thinf->socket, &res, sizeof(int));
            break;
        }
        }
    }
    close(thinf->socket);

    // bucle para hacer unmap de todos los mapas de memoria creados
    for (it = map_iter_init(topics, pos); it && map_iter_has_next(it); map_iter_next(it))
    {
        map_iter_value(it, NULL, (void **)&topic);
        munmap(topic->buf, FILE_SIZE);
    }
    map_iter_exit(it);
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 2 && argc != 3)
    {
        fprintf(stderr, "Uso: %s puerto [dir_commited]\n", argv[0]);
        return 1;
    }

    int s, s_conec, fd;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;

    // se crea el mapa
    topics = map_create(key_string, 1);

    // se inicializa la posicion del iterador
    pos = map_alloc_position(topics);

    // Necesario para la fase 5 y la practica extra
    if (argc == 3)
    {
        char *base_path = argv[2];
        struct dirent *file;

        DIR *dir = opendir(base_path);
        if (dir == NULL)
        {
            perror("El directorio no existe o no es accesible");
            return -1;
        }
        closedir(dir);

        // se crea la ruta del subdirectorio de commits
        directorio_commits = malloc(strlen(base_path) + 1 + 7 + 1);
        sprintf(directorio_commits, "%s/commits", base_path);

        // se crea en caso de que no exista
        DIR *subdir_commits = opendir(directorio_commits);
        if (subdir_commits == NULL)
        {
            if (mkdir(directorio_commits, 0700) != 0)
            {
                printf("No se ha podido crear el subdirectorio\n");
                return -1;
            }
        }
        else
            closedir(subdir_commits);

        // se crea la ruta del subdirectorio de datos
        directorio_data = malloc(strlen(base_path) + 1 + 4 + 1);
        sprintf(directorio_data, "%s/data", base_path);

        // se crea en caso de que no exista
        DIR *subdir_data = opendir(directorio_data);
        if (subdir_data == NULL)
        {
            if (mkdir(directorio_data, 0700) != 0)
            {
                printf("No se ha podido crear el subdirectorio\n");
                return -1;
            }
            subdir_data = opendir(directorio_data);
        }

        // implementacion de la recuperacion del estado
        // recorrer todos los ficheros del directorio subdir_data
        while ((file = readdir(subdir_data)) != NULL)
        {
            // saltamos los subdirectorios propio, padre, y los archivos ocultos
            if (file->d_name[0] == '.')
                continue;

            topic_t *topic;
            char ruta_fichero[BUFFER_SIZE + 1];
            char magic[5];
            int length;
            int res;

            // se crea la ruta del archivo
            sprintf(ruta_fichero, "%s/%s", directorio_data, file->d_name);

            // se abre el archivo
            fd = open(ruta_fichero, O_RDWR);
            if (fd < 0)
            {
                perror("error en open");
                return -1;
            }

            // se rellena la estructura del tema
            topic = malloc(sizeof(topic_t));
            topic->nombre = strdup(file->d_name);
            topic->cola = queue_create(0);

            // se proyecta el archivo
            topic->buf = mmap(NULL, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

            if (topic->buf == NULL)
            {
                perror("error en mmap");
                return -1;
            }

            // se comprueba que tiene el magic number
            memcpy(magic, topic->buf, 4);
            magic[4] = '\0';

            if (strcmp(magic, magic_number) != 0)
                continue;

            // se avanza el puntero
            topic->buf = (char *)topic->buf + strlen(magic_number);

            while (((char *)topic->buf)[0] != '\0')
            {
                // se obtiene el tamanyo del mensaje
                memcpy(&length, topic->buf, sizeof(int));

                // se avanza el puntero
                topic->buf = (char *)topic->buf + sizeof(int);

                // se crea el descriptor del mensaje
                message *msg = malloc(sizeof(message));
                msg->length = length;
                msg->content = topic->buf;

                // se avanza el puntero
                topic->buf = (char *)topic->buf + length;

                // se encola el mensaje
                res = queue_append(topic->cola, msg);
                if (res < 0)
                {
                    perror("append");
                    return -1;
                }
            }
            if (map_put(topics, topic->nombre, topic) < 0)
            {
                perror("no se pudo incluir en el mapa");
                return -1;
            }
            close(fd);
        }
        closedir(subdir_data);
    }

    // inicializa el socket y lo prepara para aceptar conexiones
    if ((s = init_socket_server(argv[1])) < 0)
        return -1;

    // prepara atributos adecuados para crear thread "detached"
    pthread_t thid;
    pthread_attr_t atrib_th;
    pthread_attr_init(&atrib_th); // evita pthread_join
    pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);

    while (1)
    {
        tam_dir = sizeof(dir_cliente);
        // acepta la conexión
        if ((s_conec = accept(s, (struct sockaddr *)&dir_cliente, &tam_dir)) < 0)
        {
            perror("error en accept");
            close(s);
            return -1;
        }

        // crea el thread de servicio
        thread_info *thinf = malloc(sizeof(thread_info));
        thinf->socket = s_conec;
        pthread_create(&thid, &atrib_th, servicio, thinf);
    }

    // se cierran y liberan variables usadas solo si se llama al código con 2 args
    if (argc == 3)
    {
        free(directorio_commits);
        free(directorio_data);
        close(fd);
    }

    close(s); // cierra el socket general
    return 0;
}
