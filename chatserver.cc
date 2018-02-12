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
#include <fstream>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <map>

using namespace std;

/* Consist messages and global variables */
const char* JOIN_OK = "+OK You are now in chat room #";
const char* SET_NAME = "+OK Nickname set to \'";
const char* JOINED = "-ERR You are already in room #";
const char* LEFT = "+OK You have left chat room #";
const char* UNJOINED = "-ERR Haven't joined any chat room yet.";
const char* UNKNOWN = "-ERR Unknown command.";

const int ROOM_NUM = 16;
const int MSG_LEN = 1024;
const int UNORDERED = 0;
const int FIFO = 1;
const int CAUSAL = 2;
const int TOTAL = 3;
const char NEW_MSG = 0;
const char PROPOSAL = 1;
const char AGREEMENT = 2;

/* A class for the message that deals with its info, deliverable and clock */
class Message {
private:
	int id;
	int sender;
	bool deliverable;
	vector<int> clock;
	char* message;
public:
	Message(int id, int sender) { // for totally ordered
		this->id = id;
		this->sender = sender;
		this->deliverable = false;
	}
	Message(int sender, char* clock, char* message) { // for causal ordering
		this->id = 0;
		this->sender = sender;
		this->deliverable = true;
		char* c = strtok(clock, "$");
		this->clock.push_back(atoi(c));
		while ((c = strtok(NULL, "$")) != NULL) {
			this->clock.push_back(atoi(c));
		}
		this->message = message;
	}
	int get_id() const;
	int get_sender() const;
	void set_deliverable();
	bool is_deliverable() const;
	vector<int> get_clock();
	char* get_msg();
};
int Message::get_id() const {
	return this->id;
}
int Message::get_sender() const {
	return this->sender;
}
void Message::set_deliverable() {
	this->deliverable = true;
}
bool Message::is_deliverable() const {
	return this->deliverable;
}
vector<int> Message::get_clock() {
	return this->clock;
}
char* Message::get_msg() {
	return this->message;
}

/* A class for the client that deals with its address, room and nick name */
class Client {
private:
	sockaddr_in addr;
	string nick_name;
	int room;
public:
	Client(sockaddr_in addr) {
		this->addr = addr;
		this->nick_name = "";
		this->room = -1;
	}
	sockaddr_in get_addr();
	void set_nick_name(string name);
	string get_nick_name();
	void set_room(int room);
	int get_room();
};
sockaddr_in Client::get_addr() {
	return this->addr;
}
void Client::set_nick_name(string name) {
	this->nick_name = name;
}
string Client::get_nick_name() {
	return this->nick_name;
}
void Client::set_room(int room) {
	this->room = room;
}
int Client::get_room() {
	return this->room;
}

/* Comparison structure for total order's hold-back queue */
struct Comp {
	bool operator()(const Message& m1, const Message& m2) const {
		if (m1.get_id() < m2.get_id()
				|| (m1.get_id() == m2.get_id()
						&& m1.get_sender() < m2.get_sender())) { // tie breaker with sender
			return true;
		}
		return false;
	}
};

vector<Client> CLIENTS;
vector<sockaddr_in> SERVERS;
vector<vector<unordered_map<int, string>>> FIFO_QUEUE;
vector<vector<Message>> CAUSAL_QUEUE;
vector<map<Message, string, Comp>> TOTAL_QUEUE;
vector<int> FIFO_ID;
vector<int> CLOCK;
vector<int> PROPOSED;
vector<int> AGREED;
vector<vector<int>> RECEIVED;
unsigned int listen_fd;
int SELF_IDX;
string CF_NAME;
int ORDER;
bool DEBUG;
bool RUNNING;

/* Signal handler for ctrl-c */
void sig_handler(int arg) {
	RUNNING = false;
	close(listen_fd);
	if (DEBUG) {
		printf("\nServer %d socket closed\n", SELF_IDX);
	}
}

/* Generate the head info for debug use */
string debug_str() {
	struct timeval t;
	gettimeofday(&t, NULL);
	struct tm* ptm;
	ptm = localtime(&t.tv_sec);
	char time_s[32] = { };
	char ms[16] = { };
	strftime(time_s, sizeof(time_s), "%H:%M:%S", ptm);
	sprintf(ms, ".%06ld", t.tv_sec);
	strcat(time_s, ms);
	char idx_s[6] = { };
	sprintf(idx_s, "S%02d", SELF_IDX);
	return string(time_s) + " " + string(idx_s);
}

/* Converts an ip address to a sockaddr structure */
sockaddr_in to_sockaddr(char* addr) {
	struct sockaddr_in res;
	bzero(&res, sizeof(res));
	res.sin_family = AF_INET;
	char* IP_addr = strtok(addr, ":");
	inet_pton(AF_INET, IP_addr, &(res.sin_addr));
	char* port = strtok(NULL, ":");
	res.sin_port = htons(atoi(port));
	return res;
}

/* Check if sender is a client */
bool is_client(sockaddr_in addr, int & index, int &room) {
	for (int i = 0; i < CLIENTS.size(); i++) {
		Client c = CLIENTS[i];
		sockaddr_in client = c.get_addr();
		if (strcmp(inet_ntoa(client.sin_addr), inet_ntoa(addr.sin_addr)) == 0
				&& client.sin_port == addr.sin_port) {
			index = i + 1;
			room = c.get_room();
			return true;
		}
	}
	return false;
}

/* Check if sender is a server */
bool is_server(sockaddr_in addr, int & index) {
	for (int i = 0; i < SERVERS.size(); i++) {
		sockaddr_in server = SERVERS[i];
		if (strcmp(inet_ntoa(server.sin_addr), inet_ntoa(addr.sin_addr)) == 0
				&& server.sin_port == addr.sin_port) {
			index = i + 1;
			return true;
		}
	}
	return false;
}

/* Handler for a new client */
void do_new_client(int idx, char* buffer) {
	Client &c = CLIENTS[idx - 1];
	char* tk = strtok(buffer, " ");
	string response = "";
	if (strcasecmp(tk, "/nick") == 0) { // set nick name
		char name[64] = { };
		tk = strtok(NULL, " ");
		if (tk == NULL) {
			response += "-ERR Invalid nick name.";
		} else {
			strcpy(name, tk);
			while ((tk = strtok(NULL, " ")) != NULL) {
				strcat(name, " ");
				strcat(name, tk);
			}
			c.set_nick_name(string(name));
			response += SET_NAME + string(name) + "\'";
		}
	} else if (strcasecmp(tk, "/join") == 0) { // join a chat room
		tk = strtok(NULL, " ");
		if (tk == NULL) {
			response += "-ERR Invalid chat room number.";
		} else {
			int rn = atoi(tk);
			if (rn <= 0) {
				response += "-ERR Invalid chat room number.";
			} else if (rn > ROOM_NUM) {
				response += "-ERR There are only total " + to_string(ROOM_NUM)
						+ " chat rooms.";
			} else {
				c.set_room(rn);
				response += JOIN_OK + to_string(rn);
			}
		}
	} else { // should first join a chat room
		response = UNJOINED;
	}

	const char* res = response.c_str();
	sockaddr_in addr = c.get_addr();
	sendto(listen_fd, res, strlen(res), 0, (const struct sockaddr*) &addr,
			sizeof(addr));
	if (DEBUG) {
		fprintf(stderr, "%s Server %d respond to client %d: \"%s\"\n",
				debug_str().c_str(), SELF_IDX, idx, res);
	}
}

/* Forward message to clients */
void forward_client(int room, const char* text) {
	for (int i = 0; i < CLIENTS.size(); i++) {
		Client c = CLIENTS[i];
		sockaddr_in addr = c.get_addr();
		if (c.get_room() == room) {
			sendto(listen_fd, text, strlen(text), 0,
					(const struct sockaddr*) &addr, sizeof(addr));
			if (DEBUG) {
				fprintf(stderr,
						"%s Server %d send to client %d at room %d: \"%s\"\n",
						debug_str().c_str(), SELF_IDX, i + 1, c.get_room(),
						text);
			}
		}
	}
}

/* Forward message to servers */
void forward_server(bool include, char* message) {
	for (int i = 0; i < SERVERS.size(); i++) {
		if (!include && i == SELF_IDX - 1) { // do not multicast to self except total order
			continue;
		}
		sendto(listen_fd, message, strlen(message), 0,
				(const struct sockaddr*) &SERVERS[i], sizeof(SERVERS[i]));
		if (DEBUG) {
			fprintf(stderr, "%s Server %d forward to server %d: \"%s\"\n",
					debug_str().c_str(), SELF_IDX, i + 1, message);
		}
	}
}

/* Handler for a message from client */
void do_client(int idx, char* buffer) {
	Client &c = CLIENTS[idx - 1];
	sockaddr_in addr = c.get_addr();
	string response = "";
	char cast_msg[MSG_LEN + 1] = { };
	if (buffer[0] == '/') { // command from client
		char* comm = strtok(buffer, " ");
		if (strcasecmp(comm, "/join") == 0) { // handle join chat room
			if (c.get_room() != -1) {
				response += JOINED + to_string(c.get_room());
			} else {
				char* room = strtok(NULL, " ");
				if (room == NULL) {
					response += "-ERR Invalid chat room number.";
				} else {
					int rn = atoi(room);
					if (rn <= 0) {
						response += "-ERR Invalid chat room number.";
					} else if (rn > ROOM_NUM) {
						response += "-ERR There are only total "
								+ to_string(ROOM_NUM) + " chat rooms.";
					} else {
						c.set_room(rn);
						response += JOIN_OK + to_string(rn);
					}
				}
			}
		} else if (strcasecmp(comm, "/part") == 0) { // handle leave chat room
			if (c.get_room() == -1) {
				response = UNJOINED;
			} else {
				int leave = c.get_room();
				c.set_room(-1);
				response += LEFT + to_string(leave);
			}
		} else if (strcasecmp(comm, "/nick") == 0) { // handle get nickname
			char name[64] = { };
			char* nm = strtok(NULL, " ");
			if (nm == NULL) {
				response += "-ERR Invalid nick name.";
			} else {
				strcpy(name, nm);
				while ((nm = strtok(NULL, " ")) != NULL) {
					strcat(name, " ");
					strcat(name, nm);
				}
				c.set_nick_name(string(name));
				response += SET_NAME + string(name) + "\'";
			}
		} else if (strcasecmp(comm, "/quit") == 0) { // handle quit
			CLIENTS.erase(CLIENTS.begin() + idx - 1);
			if (DEBUG) {
				fprintf(stderr, "%s Client %d quit.\n", debug_str().c_str(),
						idx);
			}
			return;
		} else {
			response = UNKNOWN;
		}

		const char* res = response.c_str();
		sendto(listen_fd, res, strlen(res), 0, (const struct sockaddr*) &addr,
				sizeof(addr));
		if (DEBUG) {
			fprintf(stderr, "%s Server %d respond to client %d: \"%s\"\n",
					debug_str().c_str(), SELF_IDX, idx, res);
		}
	} else { // chat content from client
		if (c.get_room() == -1) {
			response = UNJOINED;
			const char* res = response.c_str();
			sendto(listen_fd, res, strlen(res), 0,
					(const struct sockaddr*) &addr, sizeof(addr));
			if (DEBUG) {
				fprintf(stderr, "%s Server %d respond to client %d: \"%s\"\n",
						debug_str().c_str(), SELF_IDX, idx, res);
			}
		} else {
			string content = "";
			string type = "";
			bool include = false;
			if (c.get_nick_name() == "") { // make showing name
				char ip_name[256] = { };
				sprintf(ip_name, "%s:%d", inet_ntoa(addr.sin_addr),
						ntohs(addr.sin_port));
				content += "<" + string(ip_name) + "> " + string(buffer);
			} else {
				content += "<" + c.get_nick_name() + "> " + string(buffer);
			}
			const char* text = content.c_str();
			if (ORDER == UNORDERED || ORDER == FIFO) { // prepare for multicast to clients
				forward_client(c.get_room(), text); // a server's own messages can be directly delivered except totally ordered
				int msg_id = ++FIFO_ID[c.get_room() - 1];
				sprintf(cast_msg, "%d,%s,%d,%d,%d,%s", msg_id, "n/a", NEW_MSG,
						0, c.get_room(), text);
				type = ORDER == UNORDERED ? "Unordered" : "Fifo";
			} else if (ORDER == CAUSAL) {
				forward_client(c.get_room(), text);
				CLOCK[SELF_IDX - 1]++;
				string vc = to_string(CLOCK[0]);
				for (int i = 1; i < CLOCK.size(); i++) {
					vc += "$" + to_string(CLOCK[i]);
				}
				const char* vec = vc.c_str();
				sprintf(cast_msg, "%d,%s,%d,%d,%d,%s", 0, vec, NEW_MSG, 0,
						c.get_room(), text);
				type = "Causal";
			} else if (ORDER == TOTAL) {
				include = true;
				sprintf(cast_msg, "%d,%s,%d,%d,%d,%s", 0, "n/a", NEW_MSG, 0,
						c.get_room(), text);
				type = "Total";
			}

			forward_server(include, cast_msg); // multicast message to other servers
			if (DEBUG) {
				fprintf(stderr,
						"%s Server %d starts multicast with order: %s\n",
						debug_str().c_str(), SELF_IDX, type.c_str());
			}
		}
	}
}

/* Handler for unordered multicast */
void do_unordered(int room, char* message) {
	forward_client(room, message);
}

/* Handler for fifo multicast */
void do_fifo(int idx, int msg_id, int room, char* message) {
	int sender = idx - 1;
	int group = room - 1;
	FIFO_QUEUE[group][sender][msg_id] = string(message);
	int next = RECEIVED[group][sender] + 1;
	while (FIFO_QUEUE[group][sender].find(next)
			!= FIFO_QUEUE[group][sender].end()) {
		const char* res = FIFO_QUEUE[group][sender][next].c_str();
		forward_client(room, res);
		FIFO_QUEUE[group][sender].erase(next);
		next = (++RECEIVED[group][sender]) + 1;
	}
}

/* Handler for causal ordering multicast */
void do_causal(int idx, char* vec_cl, int room, char* message) {
	int group = room - 1;
	Message m(idx, vec_cl, message);
	CAUSAL_QUEUE[group].push_back(m);
	while (true) {
		bool progress = false;
		for (int i = 0; i < CAUSAL_QUEUE[group].size(); i++) {
			Message cur = CAUSAL_QUEUE[group][i];
			int sender = cur.get_sender() - 1;
			bool all = true;
			for (int j = 0; j < CLOCK.size(); j++) {
				if (j == sender) {
					continue;
				}
				if (cur.get_clock()[j] > CLOCK[j]) {
					all = false;
				}
			}
			if (cur.get_clock()[sender] == CLOCK[sender] + 1 && all) {
				char* res = cur.get_msg();
				forward_client(room, res);
				CLOCK[sender]++;
				CAUSAL_QUEUE[group].erase(CAUSAL_QUEUE[group].begin() + i);
				i--;
				progress = true;
			}
		}
		if (!progress) {
			break;
		}
	}
}

/* Handler for totally ordered multicast */
void do_total(int idx, int msg_id, int ord, int proby,
		unordered_map<string, vector<Message>> &proposals, int room,
		char* message) {
	int group = room - 1;
	int seq = idx - 1;
	char msg[MSG_LEN + 1] = { };
	string s = string(message);
	if (ord == NEW_MSG) { // get new message, respond with proposed number
		PROPOSED[group] = max(PROPOSED[group], AGREED[group]) + 1;
		Message m(PROPOSED[group], 0);
		TOTAL_QUEUE[group][m] = s;
		sprintf(msg, "%d,%s,%d,%d,%d,%s", PROPOSED[group], "n/a", PROPOSAL,
				SELF_IDX, room, message);
		sendto(listen_fd, msg, strlen(msg), 0,
				(const struct sockaddr*) &SERVERS[seq], sizeof(SERVERS[seq]));
	} else if (ord == PROPOSAL) { // invoker pick the highest proposed number with sender as tie breaker
		if (proposals.find(s) == proposals.end()) {
			vector<Message> new_p;
			proposals[s] = new_p;
		}
		Message m(msg_id, proby);
		proposals[s].push_back(m);
		if (proposals[s].size() == SERVERS.size()) {
			int max_id = 0;
			int max_proby = 0;
			for (int i = 0; i < proposals[s].size(); i++) {
				if (proposals[s][i].get_id() > max_id
						|| (proposals[s][i].get_id() == max_id
								&& proposals[s][i].get_sender() < max_proby)) {
					max_id = proposals[s][i].get_id();
					max_proby = proposals[s][i].get_sender();
				}
			}
			sprintf(msg, "%d,%s,%d,%d,%d,%s", max_id, "n/a", AGREEMENT,
					max_proby, room, message);
			forward_server(true, msg);
			proposals.erase(s);
		}
	} else if (ord == AGREEMENT) { // set agreed number as sequence number, update proposing number and deliver the message by sequence number
		Message m(msg_id, proby);
		m.set_deliverable();
		for (auto it : TOTAL_QUEUE[group]) {
			if (it.second.compare(s) == 0) {
				TOTAL_QUEUE[group].erase(it.first);
				TOTAL_QUEUE[group][m] = s;
			}
		}
		AGREED[group] = max(AGREED[group], msg_id);
		while (TOTAL_QUEUE[group].begin()->first.is_deliverable()) {
			strcpy(msg, TOTAL_QUEUE[group].begin()->second.c_str());
			forward_client(room, msg);
			TOTAL_QUEUE[group].erase(TOTAL_QUEUE[group].begin());
		}
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
	ORDER = UNORDERED;
	while ((ch = getopt(argc, argv, "o:v")) != -1) {
		switch (ch) {
		case 'v':
			DEBUG = true;
			break;
		case 'o':
			if (strcasecmp(optarg, "unordered") == 0) {
				ORDER = UNORDERED;
			} else if (strcasecmp(optarg, "fifo") == 0) {
				ORDER = FIFO;
			} else if (strcasecmp(optarg, "causal") == 0) {
				ORDER = CAUSAL;
			} else if (strcasecmp(optarg, "total") == 0) {
				ORDER = TOTAL;
			} else {
				fprintf(stderr, "Please enter a valid multicast order.\n");
				exit(1);
			}
			break;
		case '?':
			fprintf(stderr, "Error: Invalid choose: %c\n", (char) optopt);
			exit(1);
		default:
			fprintf(stderr,
					"Error: Please input [-o order] [-v] [configuration file] [index]\n");
			exit(1);
		}
	}
	if (optind == argc) {
		fprintf(stderr, "Error: Please input [configuration file] [index]\n");
		exit(1);
	}
	CF_NAME = argv[optind];
	optind++;
	if (optind == argc) {
		fprintf(stderr, "Error: Please input [index]\n");
		exit(1);
	}
	SELF_IDX = atoi(argv[optind]);

	struct sockaddr_in server_addr, client_addr; // Structures to represent the server and client

	/* Parse configuration file */
	ifstream cong_f;
	cong_f.open(CF_NAME);
	int i = 0;
	string ln;
	while (getline(cong_f, ln)) {
		char address[ln.length() + 1] = { };
		strcpy(address, ln.c_str());
		char* serv_ad = strtok(address, ",");
		char* binding = strtok(NULL, ",");
		SERVERS.push_back(to_sockaddr(serv_ad));
		if (i == SELF_IDX - 1) {
			if (binding != NULL) {
				server_addr = to_sockaddr(binding);
			} else {
				server_addr = SERVERS[i];
			}
		}
		i++;
	}

	/* Configure the server */
	server_addr.sin_addr.s_addr = htons(INADDR_ANY);
	/* Create a new socket */
	if ((listen_fd = socket(PF_INET, SOCK_DGRAM, 0)) == -1) {
		fprintf(stderr, "Socket open error.\n");
		exit(1);
	}
	/* Use the socket and associate it with the port number */
	if (bind(listen_fd, (const struct sockaddr *) &server_addr,
			sizeof(struct sockaddr)) == -1) {
		fprintf(stderr, "Unable to bind.\n");
		exit(1);
	}
	if (DEBUG) {
		printf("Server %d configured to listen on IP: %s, port#: %d\n",
				SELF_IDX, inet_ntoa(server_addr.sin_addr),
				ntohs(server_addr.sin_port));
	}
	fflush(stdout);

	/* Set initial chat room status */
	for (int i = 0; i < SERVERS.size(); i++) {
		CLOCK.push_back(0);
	}
	for (int i = 0; i < ROOM_NUM; i++) {
		FIFO_ID.push_back(0);
		vector<unordered_map<int, string>> fv;
		FIFO_QUEUE.push_back(fv);
		vector<Message> cv;
		CAUSAL_QUEUE.push_back(cv);
		map<Message, string, Comp> tv;
		TOTAL_QUEUE.push_back(tv);
		vector<int> rev;
		RECEIVED.push_back(rev);
		PROPOSED.push_back(0);
		AGREED.push_back(0);
		for (int j = 0; j < SERVERS.size(); j++) {
			unordered_map<int, string> ump;
			FIFO_QUEUE[i].push_back(ump);
			RECEIVED[i].push_back(0);
		}
	}
	RUNNING = true;

	unordered_map<string, vector<Message>> proposals;
	while (RUNNING) {
		char buffer[MSG_LEN + 1] = { };
		socklen_t client_len = sizeof(client_addr);
		int len = recvfrom(listen_fd, buffer, MSG_LEN, 0,
				(struct sockaddr*) &client_addr, &client_len);
		buffer[len] = 0;
		if (!RUNNING) {
			break;
		}
		int idx = 0;
		int rn = 0;
		if (is_client(client_addr, idx, rn)) { // get a message from an existing client
			if (DEBUG) {
				fprintf(stderr, "%s Client %d posts \"%s\" to chat room #%d\n",
						debug_str().c_str(), idx, buffer, rn);
			}
			do_client(idx, buffer);
		} else if (is_server(client_addr, idx)) { // get a message from another server
			if (DEBUG) {
				fprintf(stderr, "%s Server %d sends \"%s\"\n",
						debug_str().c_str(), idx, buffer);
			}
			char* mids = strtok(buffer, ",");
			int mid = atoi(mids);
			char* vcl = strtok(NULL, ",");
			char* ords = strtok(NULL, ",");
			int ord = atoi(ords);
			char* probys = strtok(NULL, ",");
			int proby = atoi(probys);
			char* rooms = strtok(NULL, ",");
			int room = atoi(rooms);
			char* msg = strtok(NULL, ",");
			if (DEBUG) {
				fprintf(stderr,
						"%s Parsed result: id: %d, clock: %s, order: %d, proposed by: %d, room: %d, message: %s\n",
						debug_str().c_str(), mid, vcl, ord, proby, room, msg);
			}
			/* Handle different multicast order */
			if (ORDER == UNORDERED) {
				do_unordered(room, msg);
			} else if (ORDER == FIFO) {
				do_fifo(idx, mid, room, msg); // mid as sequence number
			} else if (ORDER == CAUSAL) {
				do_causal(idx, vcl, room, msg);
			} else {
				do_total(idx, mid, ord, proby, proposals, room, msg); // mid as proposed number
			}
		} else { // get a message from a new client
			Client new_c(client_addr);
			CLIENTS.push_back(new_c);
			idx = CLIENTS.size();
			if (DEBUG) {
				fprintf(stderr, "%s Client %d posts \"%s\" New Client!\n",
						debug_str().c_str(), idx, buffer);
			}
			do_new_client(idx, buffer);
			if (DEBUG) {
				fprintf(stderr,
						"%s This is a new client and is accepted as %d\n",
						debug_str().c_str(), idx);
			}
		}
	}

	if (DEBUG) {
		printf("Server %d successfully shut down.\n", SELF_IDX);
	}
	return 0;
}
