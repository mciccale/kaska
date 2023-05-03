/*
 * Incluya en este fichero todas las definiciones que pueden
 * necesitar compartir el broker y la biblioteca, si es que las hubiera.
 */

#ifndef _COMUN_H
#define _COMUN_H 1

#include "map.h"
#include "queue.h"

// Se incluirán los códigos de operación
#define CREATE_TOPIC 1
#define NTOPICS 2
#define SEND_MSG 3
#define MSG_LENGTH 4
#define END_OFFSET 5
#define POLL 6
#define COMMIT 7
#define COMMITED 8

#define FILE_SIZE 1<<20

#define BUFFER_SIZE 256

typedef struct topic
{
    queue *cola;
    char *nombre;
    void *buf;
} topic_t;

typedef struct message
{
    int length;
    void *content;
} message;

typedef struct subscription
{
    char *nombre;
    int offset;
} subscription;

#endif // _COMUN_H
