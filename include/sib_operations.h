/*

  Copyright (c) 2009, Nokia Corporation
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:

    * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in
    the documentation and/or other materials provided with the
    distribution.
    * Neither the name of Nokia nor the names of its contributors
    may be used to endorse or promote products derived from this
    software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
  COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
  OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
#ifndef SIB_OPERATIONS_H
#define SIB_OPERATIONS_H

/*AD-ARCES
 * the LCTable implementation
 */
#include "LCTableTools.h"


#include "config.h"
#include <cpiglet.h>
#include <glib.h>
#include <stdlib.h>
#include <dbus/dbus.h>
#if WITH_WQL==1
#include <Python.h>
#endif /* WITH_WQL */
#include <sibdefs.h>

typedef ssStatus_t ss_status;

/* Definitions for enumerated SSAP protocol values */

typedef enum {M3_JOIN, M3_LEAVE, M3_INSERT, M3_REMOVE, M3_UPDATE,
   	      M3_QUERY, M3_SUBSCRIBE, M3_UNSUBSCRIBE, /*AD-ARCES*/ M3_PROTECTION_FAULT} transaction_type;

typedef enum {M3_REQUEST, M3_CONFIRMATION, M3_RESPONSE,
	      M3_INDICATION} message_type;

typedef QueryType query_type;

typedef EncodingType triple_encoding;

/* Enumeration for subscription states */

typedef enum {M3_SUB_ONGOING, M3_SUB_PENDING, M3_SUB_STOPPED} sub_status;

/* Struct to hold a triple using piglet's int node representation*/
typedef struct {
  gint s;
  gint p;
  gint o;
  gint dt;
  gchar* lang;
} m3_triple_int;

/* Struct to hold a node using piglet's int representation*/
typedef struct {
  gint node;
  gint dt;
  gchar* lang;
} m3_node_int;

#ifdef WITH_WQL
typedef struct {
  /* Module name */
  PyObject* p_module;
  /* Instance of wilbur DB class */
  PyObject* p_instance;
  /* Relevant methods of DB class */
  PyObject* p_add;
  PyObject* p_remove;
  PyObject* p_query;
  PyObject* p_wql_values;
  PyObject* p_wql_related;
  PyObject* p_wql_nodetypes;
  PyObject* p_wql_istype;
  PyObject* p_wql_issubtype;
} p_wilbur_functions;
#endif /* WITH_WQL */
/* Struct for holding common protocol fields */

typedef struct {
  gchar* kp_id;
  gchar* space_id;     /* !!! DAN-ARCES-2011-03.02*/
  gint tr_id;
  transaction_type tr_type;
  message_type msg_type;
} ssap_message_header;

/* Struct for holding values from messages sent by KP */

typedef struct {
  /* Req specific fields */
  gchar* credentials;
  triple_encoding encoding;
  GSList* insert_graph;
  GSList* remove_graph;
  gchar* insert_str;
  gchar* remove_str;
  query_type type;
  gchar* query_str;
  GSList* template_query;
  ssWqlDesc_t* wql_query;
  gchar* sub_id;
} ssap_kp_message;

/* Struct for holding values from messages sent by SIB */

typedef struct {
  /* Conf / rsp / ind specific fields */
  ss_status status;
  gint ind_seqnum;
  gchar* sub_id;
  GSList* bnodes;
  gchar* bnodes_str;
  GSList* results;
  gint bool_results;
  gchar* results_str;
  GSList* new_results;
  gint new_bool_results;
  gchar* new_results_str;
  GSList* obsolete_results;
  gint obsolete_bool_results;
  gchar* obsolete_results_str;
} ssap_sib_message;

/* SIB common data structures */

typedef struct {
  sub_status status;
  gchar* sub_id;
  gchar* kp_id;

  /* Lock and cond needed to sync with subscribe and unsubscribe */
  GMutex* unsub_lock;
  GCond* unsub_cond;
  gboolean unsub;
} subscription_state;

typedef struct {
  /* SS name and the node (int) in piglet*/
  gchar* ss_name;
  gint ss_node;

  /* Caches of int node - str node and str node - int node mappings */
  GHashTable* piglet_str_to_int_cache;
  GHashTable* piglet_int_to_str_cache;

  /* List of joined KPs */
  GHashTable* joined;

  /* Piglet database */
  DB RDF_store;

  /* Hash table for ongoing subscriptions */
  GHashTable* subs;

  /* Variables needed to wake up scheduler when new operations arrive */
  GCond* new_reqs_cond;
  GMutex* new_reqs_lock;
  gboolean new_reqs;

  /* Locks for common data structures */
  GMutex* members_lock;
  GMutex* subscriptions_lock;
  GMutex* store_lock;

  /* Lock for parser operations */
  /* GMutex* m3_parser_lock; */

  /* Lock for dbus send */
  /* GMutex* dbus_lock; */

  /* Lock for waiting in init until scheduler has started */
  GMutex* scheduler_init_lock;
  GCond* scheduler_init_cond;
  gboolean scheduler_init;

  /* Operation queues for op serialization */
  GAsyncQueue* insert_queue;
  GAsyncQueue* query_queue;
  /* GAsyncQueue* subscribe_queue; */

#ifdef WITH_WQL
  /* Pointers to wilbur Python functions, parameters and return values */
  p_wilbur_functions* p_w;
#endif /* WITH_WQL */
} sib_data_structure;


/* AD-ARCES
 * This modification (typedef struct SCHEDULER_ITEM{...instead:typedef struct {)
 * allow to use the scheduler item structure into the LCTaleTools and the other
 * header file that include sib_operation.h
 */

typedef struct SCHEDULER_ITEM{
  ssap_message_header* header;
  ssap_kp_message* req;
  ssap_sib_message* rsp;

  GMutex* op_lock;
  GCond* op_cond;
  gboolean op_complete;
} scheduler_item;

typedef struct {
  DBusConnection* conn;
  DBusMessage* msg;
  sib_data_structure* sib;
  transaction_type operation;
} sib_op_parameter;

typedef struct {
  /* id of joined kp*/
  gchar* kp_id;
  /* List of ongoing subscriptions for the kp */
  GSList* subs;
  gint n_of_subs;
} member_data;

sib_data_structure* sib_initialize(gchar* name);

/* Operation handler signatures */
/* These _must_ be started in a separate thread */

gpointer m3_join(gpointer data);

gpointer m3_leave(gpointer data);

gpointer m3_insert(gpointer data);

gpointer m3_remove(gpointer data);

gpointer m3_update(gpointer data);

gpointer m3_query(gpointer data);

gpointer m3_subscribe(gpointer data);

gpointer m3_unsubscribe(gpointer data);

#endif
