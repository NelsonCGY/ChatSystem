#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <iostream>
#include <cstring>

using namespace std;

unsigned int listen_fd;
bool RUNNING;
bool DEBUG;

/* Signal handler for ctrl-c */
void sig_handler(int arg) {
	RUNNING = false;
	close(listen_fd);
	if (DEBUG) {
		printf("\nClient socket closed\n");
	}
}

int main(int argc, char *argv[]) {
	if (argc < 2) {
		fprintf(stderr, "*** Author: Gongyao Chen (gongyaoc)\n");
		exit(1);
	}

	/* Your code here */

	/* Handling shutdown signal */
	signal(SIGINT, sig_handler);

	/* Parsing command line arguments */
	int ch = 0;
	while ((ch = getopt(argc, argv, "v")) != -1) {
		switch (ch) {
		case 'v':
			DEBUG = true;
			break;
		case '?':
			fprintf(stderr, "Error: Invalid choose: %c\n", (char) optopt);
			exit(1);
		default:
			fprintf(stderr,
					"Error: Please input [IP address:port number] [-v]\n");
			exit(1);
		}
	}
	if (optind == argc) {
		fprintf(stderr, "Error: Please input [IP address:port number]\n");
		exit(1);
	}

	/* Parse IP address and port number */
	char* IP_addr = strtok(argv[optind], ":");
	if (!IP_addr) {
		fprintf(stderr, "Invalid [IP address:port number]\n");
		exit(1);
	}
	char* port = strtok(NULL, ":");
	if (!port || atoi(port) < 0) {
		fprintf(stderr, "Invalid [port number]\n");
		exit(1);
	}
	unsigned int port_N = atoi(port);

	struct sockaddr_in server_addr, client_addr; // Structures to represent the server and client

	/* Set up client socket */
	if ((listen_fd = socket(PF_INET, SOCK_DGRAM, 0)) == -1) {
		fprintf(stderr, "Socket open error.\n");
		exit(1);
	}
	bzero(&client_addr, sizeof(client_addr));
	client_addr.sin_family = AF_INET;
	client_addr.sin_port = htons(port_N);
	inet_pton(AF_INET, IP_addr, &(client_addr.sin_addr));
	if (DEBUG) {
		fprintf(stdout, "Client configured to IP: %s, port#: %d\n", IP_addr,
				port_N);
	}
	fflush(stdout);
	RUNNING = true;

	/* Set up selection reading */
	fd_set readfds;
	struct timeval timeout;

	while (RUNNING) {
		/* Set up client monitoring */
		socklen_t server_len = sizeof(server_addr);
		FD_ZERO(&readfds);
		FD_SET(listen_fd, &readfds);
		FD_SET(STDIN_FILENO, &readfds);
		timeout.tv_sec = 10;
		timeout.tv_usec = 0;
		int res = select(listen_fd + 1, &readfds, NULL, NULL, NULL);
		if (res <= 0) {
			if (DEBUG) {
				fprintf(stderr, "Client waiting income...\n");
			}
			continue;
		}

		/* Handle income message */
		char buffer[1025] = { };
		int len = 0;
		if (FD_ISSET(STDIN_FILENO, &readfds)) {
			len = read(STDIN_FILENO, buffer, 1024);
			buffer[len - 1] = 0; // get away the \n
			sendto(listen_fd, buffer, strlen(buffer), 0,
					(const struct sockaddr*) &client_addr, sizeof(client_addr));
			char* token = strtok(buffer, " ");
			if (strcasecmp(token, "/quit") == 0) {
				RUNNING = false;
				close(listen_fd);
				if (DEBUG) {
					printf("\nClient socket closed\n");
				}
				break;
			}
		} else {
			len = recvfrom(listen_fd, buffer, 1024, 0,
					(struct sockaddr*) &server_addr, &server_len);
			buffer[len] = 0;
			fprintf(stdout, "%s\n", buffer);
		}
		memset(buffer, 0, 1025);
	}

	if (DEBUG) {
		printf("Client successfully shut down.\n");
	}
	return 0;
}
