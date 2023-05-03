// EJEMPLO DE CLIENTE QUE ENVÍA CON UNA SOLA OPERACIÓN UN ENTERO,
// UN STRING Y UN ARRAY BINARIO. PARA ENVIAR ESTOS DOS ÚLTIMOS, AL
// SER DE TAMAÑO VARIABLE, TRANSMITE ANTES SU TAMAÑO.
// PARA SIMULAR VARIAS PETICIONES, REPITE VARIAS VECES ESA ACCIÓN.
// PUEDE USARLO COMO BASE PARA LA BIBLIOTECA DE CLIENTE.
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#define NUM_REQ 3 // REPITE VARIAS VECES (COMO SI FUERAN MÚLTIPLES PETICIONES)
		  
// inicializa el socket y se conecta al servidor
static int init_socket_client(const char *host_server, const char * port) {
    int s;
    struct addrinfo *res;
    // socket stream para Internet: TCP
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }
    // obtiene la dirección TCP remota
    if (getaddrinfo(host_server, port, NULL, &res)!=0) {
        perror("error en getaddrinfo");
        close(s);
        return -1;
    }
    // realiza la conexión
    if (connect(s, res->ai_addr, res->ai_addrlen) < 0) {
        perror("error en connect");
        close(s);
        return -1;
    }
    freeaddrinfo(res);
    return s;
}

static int peticion(int s, int entero, char *string, int longitud_array,
		void *array) {
        struct iovec iov[5]; // hay que enviar 5 elementos
        int nelem = 0;
        
        // preparo el envío del entero convertido a formato de red
        int entero_net = htonl(entero);
        iov[nelem].iov_base=&entero_net;
        iov[nelem++].iov_len=sizeof(int);

        // preparo el envío del string mandando antes su longitud
        int longitud_str = strlen(string); // no incluye el carácter nulo
        int longitud_str_net = htonl(longitud_str);
        iov[nelem].iov_base=&longitud_str_net;
        iov[nelem++].iov_len=sizeof(int);
        iov[nelem].iov_base=string; // no se usa & porque ya es un puntero
        iov[nelem++].iov_len=longitud_str;

        // preparo el envío del array mandando antes su longitud
        int longitud_arr_net = htonl(longitud_array);
        iov[nelem].iov_base=&longitud_arr_net;
        iov[nelem++].iov_len=sizeof(int);
        iov[nelem].iov_base=array; // no se usa & porque ya es un puntero
        iov[nelem++].iov_len=longitud_array;

        // modo de operación de los sockets asegura que si no hay error
        // se enviará todo (misma semántica que los "pipes")
        if (writev(s, iov, 5)<0) {
            perror("error en writev");
            close(s);
            return -1;
        }
	int res;
	// recibe un entero como respuesta
	if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("error en recv");
            close(s);
            return -1;
        }
        return ntohl(res);
}

int main(int argc, char *argv[]) {
    int s;
    if (argc!=3) {
        fprintf(stderr, "Uso: %s servidor puerto\n", argv[0]);
        return -1;
    }
    // inicializa el socket y se conecta al servidor
    if ((s=init_socket_client(argv[1], argv[2]))<0) return -1;

    // datos a enviar
    int entero = 12345;
    char *string = "abcdefghijklmnopqrstuvwxyz";
    // podría ser una imagen, un hash, texto cifrado...
    unsigned char array_binario[] = {0x61, 0x62, 0x0, 0x9, 0xa, 0x41, 0x42};
    int res;

    // los envía varias veces como si fueran sucesivas peticiones del cliente
    // que mantiene una conexión persistente
    for (int i=0; i<NUM_REQ; i++) {
	// cambio algunos valores para la próxima "petición"
	if ((res=peticion(s, entero, string, sizeof(array_binario),
		array_binario))==-1)
	    break;
        printf("Recibida respuesta: %d\n", ntohl(res));

	++entero;
	++array_binario[0];
    }
    close(s); // cierra el socket
    return 0;
}
