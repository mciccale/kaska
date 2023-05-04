#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <stdbool.h>

#include "comun.h"
#include "kaska.h"
#include "map.h"

int s;                     // socket único por sesión
int entero;                // variable util
int result;                // variable para almacenar resultados
int my_offset;             // variable offset
int trash;                 // variable para tirar
bool s_bound = false;      // variable que indica si está el socket ya creado
bool found;                // variable para poll
map *subscriptions = NULL; // mapa para guardar a qué temas se está suscrito
map_position *pos;         // variable para el iterador
map_iter *it;              // iterador del mapa
subscription *sub;         // variable para la subscripcion

static int init_socket_client()
{
    struct addrinfo *res;

    // socket stream para Internet: TCP
    if ((s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    {
        perror("error creando socket");
        return -1;
    }

    s_bound = true;

    // obtiene la dirección TCP remota
    if (getaddrinfo(getenv("BROKER_HOST"), getenv("BROKER_PORT"), NULL, &res) != 0)
    {
        perror("error en getaddrinfo");
        close(s);
        s_bound = false;
        return -1;
    }

    // realiza la conexión
    if (connect(s, res->ai_addr, res->ai_addrlen) < 0)
    {
        perror("error en connect");
        close(s);
        s_bound = false;
        return -1;
    }

    freeaddrinfo(res);
    return s;
}
static void imprime_entrada_suscripciones(void *k, void *v, void *d)
{
    subscription *sub = (subscription *)v;
    printf("después de subscribe: nombre %s offset %d\n", (char *)k, sub->offset);
}

// Crea el tema especificado.
// Devuelve 0 si OK y un valor negativo en caso de error.
int create_topic(char *topic)
{
    struct iovec iov[3]; // hay que enviar 3 elementos
    int longitud = strlen(topic);
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(CREATE_TOPIC);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se prepara la longitud del string
    int longitud_net = htonl(longitud);
    iov[1].iov_base = &longitud_net;
    iov[1].iov_len = sizeof(int);

    // se prepara la cadena del tema
    iov[2].iov_base = topic;
    iov[2].iov_len = longitud;

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 3) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe un entero como respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }
    return ntohl(res);
}
// Devuelve cuántos temas existen en el sistema y un valor negativo
// en caso de error.
int ntopics(void)
{
    struct iovec iov[1]; // hay que enviar 1 elemento
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(NTOPICS);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 1) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe la cantidad de temas
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }
    return ntohl(res);
}

// SEGUNDA FASE: PRODUCIR/PUBLICAR

// Envía el mensaje al tema especificado; nótese la necesidad
// de indicar el tamaño ya que puede tener un contenido de tipo binario.
// Devuelve el offset si OK y un valor negativo en caso de error.
int send_msg(char *topic, int msg_size, void *msg)
{
    struct iovec iov[5]; // hay que enviar 3 elementos
    int longitud = strlen(topic);
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(SEND_MSG);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se prepara la longitud del tema
    int longitud_tema_net = htonl(longitud);
    iov[1].iov_base = &longitud_tema_net;
    iov[1].iov_len = sizeof(int);

    // se prepara la cadena del tema
    iov[2].iov_base = topic;
    iov[2].iov_len = longitud;

    // se prepara la longitud del mensaje
    int longitud_msg_net = htonl(msg_size);
    iov[3].iov_base = &longitud_msg_net;
    iov[3].iov_len = sizeof(int);

    // se prepara el mensaje
    iov[4].iov_base = msg;
    iov[4].iov_len = msg_size;

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 5) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe el offset del mensaje dentro de la cola
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }
    return ntohl(res);
}
// Devuelve la longitud del mensaje almacenado en ese offset del tema indicado
// y un valor negativo en caso de error.
int msg_length(char *topic, int offset)
{
    struct iovec iov[4]; // hay que enviar 4 elementos
    int longitud = strlen(topic);
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(MSG_LENGTH);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se prepara la longitud del tema
    int longitud_tema_net = htonl(longitud);
    iov[1].iov_base = &longitud_tema_net;
    iov[1].iov_len = sizeof(int);

    // se prepara la cadena del tema
    iov[2].iov_base = topic;
    iov[2].iov_len = longitud;

    // se prepara el offset
    int offset_net = htonl(offset);
    iov[3].iov_base = &offset_net;
    iov[3].iov_len = sizeof(int);

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 4) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe la longitud del mensaje del tema "topic" en el offset "offset"
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }
    return ntohl(res);
}
// Obtiene el último offset asociado a un tema en el broker, que corresponde
// al del último mensaje enviado más uno y, dado que los mensajes se
// numeran desde 0, coincide con el número de mensajes asociados a ese tema.
// Devuelve ese offset si OK y un valor negativo en caso de error.
int end_offset(char *topic)
{
    struct iovec iov[3]; // hay que enviar 3 elementos
    int longitud = strlen(topic);
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(END_OFFSET);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se prepara la longitud del string
    int longitud_net = htonl(longitud);
    iov[1].iov_base = &longitud_net;
    iov[1].iov_len = sizeof(int);

    // se prepara la cadena del tema
    iov[2].iov_base = topic;
    iov[2].iov_len = longitud;

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 3) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe el tamaño de la cola de mensajes asociado al tema "topic"
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }
    return ntohl(res);
}

// TERCERA FASE: SUBSCRIPCIÓN

// Se suscribe al conjunto de temas recibidos. No permite suscripción
// incremental: hay que especificar todos los temas de una vez.
// Si un tema no existe o está repetido en la lista simplemente se ignora.
// Devuelve el número de temas a los que realmente se ha suscrito
// y un valor negativo solo si ya estaba suscrito a algún tema.
int subscribe(int ntopics, char **topics)
{
    int count_subs = 0;
    if (subscriptions != NULL)
        return -1;

    // se crea el mapa de suscripciones en modo "not sharing"
    subscriptions = map_create(key_string, 0);

    // se obtiene el offset de cada tema en caso de que exista y
    // se intenta añadir al mapa K = topic, V = {topic, offset},
    // solo si se ha añadido, se añade al contador de suscripciones
    // realizadas
    for (int i = 0; i < ntopics; ++i)
    {
        my_offset = end_offset(topics[i]);
        if (my_offset == -1)
            continue;
        sub = malloc(sizeof(subscription));
        sub->nombre = strdup(topics[i]);
        sub->offset = my_offset;
        if (map_put(subscriptions, sub->nombre, sub) == 0)
            count_subs++;
    }

    // se inicializa la variable de iteración
    pos = map_alloc_position(subscriptions);

    // se imprime el contenido del mapa
    map_visit(subscriptions, imprime_entrada_suscripciones, NULL);
    return count_subs;
}

// Se da de baja de todos los temas suscritos.
// Devuelve 0 si OK y un valor negativo si no había suscripciones activas.
int unsubscribe(void)
{
    if (subscriptions == NULL)
        return -1;

    map_destroy(subscriptions, NULL);
    map_free_position(pos);
    subscriptions = NULL;
    return 0;
}

// Devuelve el offset del cliente para ese tema y un número negativo en
// caso de error.
int position(char *topic)
{
    sub = (subscription *)map_get(subscriptions, topic, &entero);
    if (entero < 0)
        return -1;

    return sub->offset;
}

// Modifica el offset del cliente para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int seek(char *topic, int offset)
{
    sub = (subscription *)map_get(subscriptions, topic, &entero);
    if (entero < 0)
        return -1;

    sub->offset = offset;
    return 0;
}

// CUARTA FASE: LEER MENSAJES

// Obtiene el siguiente mensaje destinado a este cliente; los dos parámetros
// son de salida.
// Devuelve el tamaño del mensaje (0 si no había mensaje)
// y un número negativo en caso de error.
int poll(char **topic, void **msg)
{
    void *mensaje;
    found = false;

    for (it = map_iter_init(subscriptions, pos); it && !found && map_iter_has_next(it); map_iter_next(it))
    {
        map_iter_value(it, NULL, (void **)&sub);

        struct iovec iov[4]; // hay que enviar 4 elementos
        int longitud = strlen(sub->nombre);

        // se prepara el código de operación
        int cod_op_net = htonl(POLL);
        iov[0].iov_base = &cod_op_net;
        iov[0].iov_len = sizeof(int);

        // se prepara la longitud del tema
        int longitud_tema_net = htonl(longitud);
        iov[1].iov_base = &longitud_tema_net;
        iov[1].iov_len = sizeof(int);

        // se prepara la cadena del tema
        iov[2].iov_base = sub->nombre;
        iov[2].iov_len = longitud;

        // se prepara el offset
        int offset_net = htonl(sub->offset);
        iov[3].iov_base = &offset_net;
        iov[3].iov_len = sizeof(int);

        // se crea la conexion y se envían los datos
        if (!s_bound)
            s = init_socket_client();
        if (writev(s, iov, 4) < 0)
        {
            perror("error en writev");
            close(s);
            s_bound = false;
            return -1;
        }

        // se recibe primero un entero
        // -1: No existe el tema
        //  0: No hay un mensaje en ese tema con ese offset
        //  N: Longitud del mensaje
        if (recv(s, &result, sizeof(int), MSG_WAITALL) != sizeof(int))
        {
            perror("error en recv");
            close(s);
            s_bound = false;
            return -1;
        }

        result = ntohl(result);
        if (result > 0)
        {
            found = true;
            continue;
        }
        else
        {
            if (recv(s, &trash, sizeof(int), MSG_WAITALL) != sizeof(int))
            {
                perror("error en recv");
                close(s);
                s_bound = false;
                return -1;
            }
        }
    }

    if (result > 0)
    {
        // se reserva el espacio necesario en el puntero
        mensaje = malloc(result);

        // se recibe el mensaje
        if (recv(s, mensaje, result, MSG_WAITALL) != result)
        {
            perror("error en recv");
            close(s);
            s_bound = false;
            return -1;
        }

        // se crea un duplicado del nombre del tema
        *topic = strdup(sub->nombre);
        *msg = mensaje;

        // se sale del iterador guardando el estado del mismo
        pos = map_iter_exit(it);

        // se ha de incrementar el offset del cliente
        sub->offset++;

        return result;
    }
    pos = map_iter_exit(it);
    return 0;
}

// QUINTA FASE: COMMIT OFFSETS

// Cliente guarda el offset especificado para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int commit(char *client, char *topic, int offset)
{
    struct iovec iov[6]; // hay que enviar 6 elementos
    int longitud_cliente = strlen(client);
    int longitud_tema = strlen(topic);
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(COMMIT);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se prepara la longitud del cliente
    int longitud_cliente_net = htonl(longitud_cliente);
    iov[1].iov_base = &longitud_cliente_net;
    iov[1].iov_len = sizeof(int);

    // se prepara la cadena del cliente
    iov[2].iov_base = client;
    iov[2].iov_len = longitud_cliente;

    // se prepara la longitud del tema
    int longitud_tema_net = htonl(longitud_tema);
    iov[3].iov_base = &longitud_tema_net;
    iov[3].iov_len = sizeof(int);

    // se prepara la cadena del tema
    iov[4].iov_base = topic;
    iov[4].iov_len = longitud_tema;

    // se prepara el offset
    int offset_net = htonl(offset);
    iov[5].iov_base = &offset_net;
    iov[5].iov_len = sizeof(int);

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 6) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe la confirmacion de la operacion
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }

    return ntohl(res);
}

// Cliente obtiene el offset guardado para ese tema.
// Devuelve el offset y un número negativo en caso de error.
int commited(char *client, char *topic)
{
    struct iovec iov[5]; // hay que enviar 6 elementos
    int longitud_cliente = strlen(client);
    int longitud_tema = strlen(topic);
    int res;

    // se prepara el código de operación
    int cod_op_net = htonl(COMMITED);
    iov[0].iov_base = &cod_op_net;
    iov[0].iov_len = sizeof(int);

    // se prepara la longitud del cliente
    int longitud_cliente_net = htonl(longitud_cliente);
    iov[1].iov_base = &longitud_cliente_net;
    iov[1].iov_len = sizeof(int);

    // se prepara la cadena del cliente
    iov[2].iov_base = client;
    iov[2].iov_len = longitud_cliente;

    // se prepara la longitud del tema
    int longitud_tema_net = htonl(longitud_tema);
    iov[3].iov_base = &longitud_tema_net;
    iov[3].iov_len = sizeof(int);

    // se prepara la cadena del tema
    iov[4].iov_base = topic;
    iov[4].iov_len = longitud_tema;

    // se crea la conexion y se envían los datos
    if (!s_bound)
        s = init_socket_client();
    if (writev(s, iov, 5) < 0)
    {
        perror("error en writev");
        close(s);
        s_bound = false;
        return -1;
    }

    // se recibe el offset
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int))
    {
        perror("error en recv");
        close(s);
        s_bound = false;
        return -1;
    }

    return ntohl(res);
}
