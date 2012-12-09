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
#include <glib.h>
#include <stdio.h>
#include <cpiglet.h>

#include <sib_dbus_ifaces.h>
#include <whiteboard_util.h>
#include <whiteboard_log.h>
#include <sibdefs.h>

#define SIB_ROLE

#include <sibmsg.h>

#include "sib_operations.h"

#if WITH_WQL==1
#define PYTHON_WILBUR_MODULE "rdfplus_m3"
#endif /* WITH_WQL */

char* PIGLET_ERR_DB_OPEN = "Unable to open database";
char* PIGLET_ERR_NODE_ID = "Unable to create a new node ID";
char* PIGLET_ERR_NODE_DETAILS = "Unable to query for node details";
char* PIGLET_ERR_NODE_NEW = "Unable to insert a new node";
char* PIGLET_ERR_NODE_FIND = "Unable to find node";
char* PIGLET_ERR_TRIPLE_ADD = "Unable to insert new triple";
char* PIGLET_ERR_TRIPLE_DEL = "Unable to delete triple";
char* PIGLET_ERR_TRIPLE_FIND = "Unable to query for triples";
char* PIGLET_ERR_NS_ADD = "Unable to insert new namespace";
char* PIGLET_ERR_NS_DEL = "Unable to delete namespace";
char* PIGLET_ERR_NS_FIND = "Unable to find namespace";
char* PIGLET_ERR_SRC_FIND = "Unable to determine if source has been loaded before";
char* PIGLET_ERR_SRC_TIME = "Unable to update load time";
char* PIGLET_ERR_SRC_DEL = "Unable to update load time";
char* PIGLET_ERR_SRC_QUERY = "Unable to find sources";
char* PIGLET_ERR_TRANSACTION = "Transaction-related error";

extern charStr SIB_TRIPLELIST;
extern charStr SIB_NODE_LIST;

extern void ssFreeTriple (ssTriple_t *triple);
extern void ssFreeTripleList (GSList **tripleList);

extern void ssFreePathNode (ssPathNode_t *pathNode);
extern void ssFreePathNodeList (GSList **pathNodeList);

typedef struct {
  GAsyncQueue *insert_queue;
  GAsyncQueue *query_queue;

  GHashTable *subs;
  GMutex *subscriptions_lock;

  GCond *new_reqs_cond;
  GMutex *new_reqs_mutex;
  gboolean new_reqs;
} scheduler_param;


#if WITH_WQL==1

typedef struct {
  PyObject_HEAD
  DB db;
} PyPiglet_DBObject;

#define asDB(x) (((PyPiglet_DBObject *)x)->db)

DB p_call_get_db(p_wilbur_functions *w)
{
  PyPiglet_DBObject* p_db;
  p_db = (PyPiglet_DBObject*)PyObject_CallMethod(w->p_instance, "m3_get_db", NULL);
  if (p_db == NULL || asDB(p_db) == NULL)
    {
      PyErr_Print();
      return NULL;
    }
  return asDB(p_db);
}

void p_call_add(p_wilbur_functions *w, gint s, gint p, gint o)
{
  PyObject_CallMethod(w->p_instance, "add", "(iii)", s, p, o);
}

void p_call_delete(p_wilbur_functions *w, gint s, gint p, gint o)
{
  PyObject_CallMethod(w->p_instance, "delete", "(iii)", s, p, o);
}


gchar* p_call_info(p_wilbur_functions *w, gint node)
{
  PyObject *t;
  gchar *result;
  t = PyObject_CallMethod(w->p_instance, "info", "(i)", node);
  result = PyString_AsString(t);
  Py_DECREF(t);
  return result;
}

gint p_call_node(p_wilbur_functions *w, gchar *node)
{
  PyObject *t;
  gint result;
  t = PyObject_CallMethod(w->p_instance, "node", "(s)", node);
  result = (gint)PyInt_AsLong(t);
  Py_DECREF(t);
  return result;
}

gchar* p_call_expand_m3(p_wilbur_functions *w, gchar *node)
{
  PyObject *t;
  gchar *result;
  t = PyObject_CallMethod(w->p_instance, "expand_m3", "(s)", node);
  result = PyString_AsString(t);
  Py_DECREF(t);
  return result;
}

GSList* p_call_query(p_wilbur_functions *w, gint s, gint p, gint o)
{
  PyObject *p_r, *p_t, *p_tmp;
  GSList *result = NULL;
  m3_triple_int *triple;
  p_r = PyObject_CallMethod(w->p_instance, "query", "(iii)", s, p, o);
  if (!PyIter_Check(p_r))
    return NULL;
  for (p_t = PyIter_Next(p_r); p_t != NULL; p_t = PyIter_Next(p_r))
    {
      triple = g_new0(m3_triple_int, 1);
      p_tmp = PyTuple_GetItem(p_t, 0);
      triple->s = (gint)PyInt_AsLong(p_tmp);
      p_tmp = PyTuple_GetItem(p_t, 1);
      triple->p = (gint)PyInt_AsLong(p_tmp);
      p_tmp = PyTuple_GetItem(p_t, 2);
      triple->o = (gint)PyInt_AsLong(p_tmp);
      Py_DECREF(p_t);
      if (triple->o < 0) {
	triple->lang = g_strdup("");
	triple->dt = 0;
      }
      result = g_slist_prepend(result, (gpointer)triple);
    }
  return result;
}


GSList* p_call_values(p_wilbur_functions *w, gint node, gchar *path)
{
  PyObject *p_r, *p_n;
  GSList *result = NULL;
  m3_node_int *result_node;

  p_r = PyObject_CallMethod(w->p_instance, "values_m3", "(is)", node, path);
  whiteboard_log_debug("Proceeded after wql values call\n");
  if (NULL == p_r)
    {
      whiteboard_log_debug("Wql values call result was NULL!\n");
      PyErr_Print();
      return NULL;
    }
  if (!PyIter_Check(p_r))
    {
      whiteboard_log_debug("Wql values result was not iterable!\n");
      PyErr_Print();
      return NULL;
    }
  for (p_n = PyIter_Next(p_r); p_n != NULL; p_n = PyIter_Next(p_r))
    {
      result_node = g_new0(m3_node_int, 1);
      result_node->node = (gint)PyInt_AsLong(p_n);
      Py_DECREF(p_n);
      if (result_node->node < 0) {
	result_node->lang = NULL;
	result_node->dt = 0;
      }
      /* whiteboard_log_debug("Got node %d\n", result_node->node); */
      result = g_slist_prepend(result, (gpointer)result_node);
    }
  return result;
}

GSList* p_call_nodetypes(p_wilbur_functions *w, gint node)
{
  PyObject *p_r, *p_n;
  GSList *result = NULL;
  m3_node_int *result_node;
  p_r = PyObject_CallMethod(w->p_instance, "nodeTypes_m3", "(i)", node);
  if (NULL == p_r)
    {
      whiteboard_log_debug("Wql nodeTypes call result was NULL!\n");
      PyErr_Print();
      return NULL;
    }
  if (!PyIter_Check(p_r))
    {
      whiteboard_log_debug("Wql nodeTypes result was not iterable!\n");
      PyErr_Print();
      return NULL;
    }
  for (p_n = PyIter_Next(p_r); p_n != NULL; p_n = PyIter_Next(p_r))
    {
      result_node = g_new0(m3_node_int, 1);
      result_node->node = (gint)PyInt_AsLong(p_n);
      Py_DECREF(p_n);
      /* Result should never be a literal, this could be removed */
      if (result_node->node < 0) {
	result_node->lang = NULL;
	result_node->dt = 0;
      }
      result = g_slist_prepend(result, (gpointer)result_node);
    }
  return result;
}

gint p_call_istype(p_wilbur_functions* w, gint node, gint type)
{
  PyObject *p_r;
  gint result = false;
  p_r = PyObject_CallMethod(w->p_instance, "isType", "(ii)", node, type);
  if (NULL == p_r)
    {
      whiteboard_log_debug("Wql isType call result was NULL!\n");
      PyErr_Print();
      return 0;
    }
  result = PyInt_AsLong(p_r);
  Py_DECREF(p_r);
  return result;
}

gint p_call_related(p_wilbur_functions* w,
		    gint source,
		    gchar* path,
		    gint sink)
{
  PyObject *p_r;
  gint result = false;
  p_r = PyObject_CallMethod(w->p_instance, "related_m3", "(isi)", source, path, sink);
  if (NULL == p_r)
    {
      whiteboard_log_debug("Wql related call result was NULL!\n");
      PyErr_Print();
    }
  else
    {
      result = PyInt_AsLong(p_r);
      Py_DECREF(p_r);
    }
  return result;
}

gint p_call_issubtype(p_wilbur_functions* w, gint subtype, gint type)
{
  PyObject *p_r;
  gint result = false;
  p_r = PyObject_CallMethod(w->p_instance, "isSubtype", "(ii)", subtype, type);
  if (NULL == p_r)
    {
      whiteboard_log_debug("Wql isSubtype call result was NULL!\n");
      PyErr_Print();
      return 0;
    }
  result = PyInt_AsLong(p_r);
  Py_DECREF(p_r);
  return result;
}

#endif /* WITH_WQL */

gpointer m3_join(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  GHashTable* joined;
  gchar* kp_id;
  gchar *credentials_rsp;
  member_data* kp_data;

  sib_op_parameter* param = (sib_op_parameter*) data;
  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);


  joined = param->sib->joined;
  /* Initialize message structs */
  header->tr_type = M3_JOIN;
  header->msg_type = M3_REQUEST;

  /* Parse message */

  if(whiteboard_util_parse_message(param->msg,
			    DBUS_TYPE_STRING, &(header->space_id),
			    DBUS_TYPE_STRING, &(header->kp_id),
			    DBUS_TYPE_INT32, &(header->tr_id),
			    DBUS_TYPE_STRING, &(req_msg->credentials),
			    DBUS_TYPE_INVALID) )
    {

      /* Update the joined KPs hash table*/
      kp_data = g_new0(member_data, 1);
      kp_data->kp_id = g_strdup(header->kp_id);
      kp_data->subs = NULL;
      kp_data->n_of_subs = 0;
      kp_id = g_strdup(header->kp_id);

      /* LOCK MEMBER KP TABLE */
      g_mutex_lock(param->sib->members_lock);

      if (!g_hash_table_lookup(joined, kp_id))
	{
	  g_hash_table_insert(joined, kp_id, (gpointer)kp_data);
	  /* UNLOCK MEMBER KP TABLE */
	  g_mutex_unlock(param->sib->members_lock);
	}
      else
	{
	  /* UNLOCK MEMBER KP TABLE */
	  g_mutex_unlock(param->sib->members_lock);
	  rsp_msg->status = ss_KPErrorRequest;
	  credentials_rsp = g_strdup("m3:KPErrorRequest");
	  goto send_response;
	}


      whiteboard_log_debug("KP %s joined the smart space\n", header->kp_id);

      credentials_rsp = g_strdup("m3:Success");
      rsp_msg->status = ss_StatusOK;
    send_response:
      whiteboard_util_send_method_return(param->conn,
				  param->msg,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_INT32, &(rsp_msg->status),
				  DBUS_TYPE_STRING, &credentials_rsp,
				  WHITEBOARD_UTIL_LIST_END);
      whiteboard_log_debug("Sent response with status %d\n", rsp_msg->status);
      g_free(credentials_rsp);
      g_free(header);
      g_free(req_msg);
      g_free(rsp_msg);

    }
  else
    {
      whiteboard_log_warning("Could not parse JOIN method call message\n");
    }
  dbus_message_unref(param->msg);
  g_free(param);

  return NULL;
}

gpointer m3_leave(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  GHashTable* joined;
  sib_op_parameter* param = (sib_op_parameter*) data;
  member_data* kp_data;
  /* gchar* sub_id; */
  GSList* i;
  /* subscription_state* sub; */


  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);


  joined = param->sib->joined;
  /* Initialize message structs */
  header->tr_type = M3_LEAVE;
  header->msg_type = M3_REQUEST;

  /* Parse message */

  if(whiteboard_util_parse_message(param->msg,
			    DBUS_TYPE_STRING, &(header->space_id),
			    DBUS_TYPE_STRING, &(header->kp_id),
			    DBUS_TYPE_INT32, &(header->tr_id),
			    DBUS_TYPE_INVALID) )
    {
      /* Update the joined KPs hash table*/

      /* LOCK MEMBER KP TABLE */
      g_mutex_lock(param->sib->members_lock);
      kp_data = g_hash_table_lookup(joined, header->kp_id);
      if (NULL == kp_data)
	{
	  g_mutex_unlock(param->sib->members_lock);
	  rsp_msg->status = ss_KPErrorRequest;
	  goto send_response;
	}

      /* TODO: Stopping of subscriptions when leaving
      if (kp_data != NULL)
	{
	  if (kp_data->n_of_subs > 0)
	    {
	      for (i = kp_data->subs; i != NULL; i = i->next)
		{
		  sub_id = i->data;
		  g_mutex_lock(param->sib->subscriptions_lock);
		  sub = (subscription_state*)g_hash_table_lookup(param->sib->subs, req_msg->sub_id);
		  g_mutex_unlock(param->sib->subscriptions_lock);
		}

	    }
	}
      */
      g_free(kp_data->kp_id);
      for (i = kp_data->subs; i != NULL; i = i->next)
	g_free(i->data);
      g_hash_table_remove(joined, header->kp_id);
      if (kp_data)
	g_free(kp_data);

      /* UNLOCK MEMBER KP TABLE */
      g_mutex_unlock(param->sib->members_lock);

      rsp_msg->status = ss_StatusOK;

      whiteboard_log_debug("KP %s left the smart space\n", header->kp_id);

    send_response:
      whiteboard_util_send_method_return(param->conn,
				  param->msg,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_INT32, &(rsp_msg->status),
				  WHITEBOARD_UTIL_LIST_END);
      g_free(req_msg);
      g_free(rsp_msg);
      g_free(header);
    }

  else
    {
      whiteboard_log_warning("Could not parse LEAVE method call message\n");
    }
  dbus_message_unref(param->msg);
  g_free(param);
  return NULL;

}

gpointer m3_insert(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;
  ssStatus_t status = ss_StatusOK;
  sib_op_parameter* param = (sib_op_parameter*) data;

  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);
  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();
  rsp_msg->status = ss_StatusOK;    /* JUKKA - ARCES */

  if(whiteboard_util_parse_message(param->msg,
				   DBUS_TYPE_STRING, &(header->space_id),
				   DBUS_TYPE_STRING, &(header->kp_id),
				   DBUS_TYPE_INT32, &(header->tr_id),
				   DBUS_TYPE_INT32, &(req_msg->encoding),
				   DBUS_TYPE_STRING, &(req_msg->insert_str),
				   DBUS_TYPE_INVALID) )
    {
      whiteboard_log_debug("Parsed insert %d\n", header->tr_id);
      /* Initialize message structs */
      header->tr_type = M3_INSERT;
      header->msg_type = M3_REQUEST;
      // printf("INSERT: got insert msg: %s\n", req_msg->insert_str);

      /* Parse M3 triples here, RDF/XML is parsed in Piglet */
      if (EncodingM3XML == req_msg->encoding)
	{
	  status = parseM3_triples_SIB(&(req_msg->insert_graph),
				       req_msg->insert_str,
				       NULL,
				       &(rsp_msg->bnodes_str));
	  // printf("INSERT: parsed bnodes str: %s\n", rsp_msg->bnodes_str);
	  whiteboard_log_debug("Parsed M3 RDF in insert %d\n", header->tr_id);
	  if (status != ss_StatusOK)
	    {
	      /*rsp_msg->status = ss_KPErrorMsgSyntax;*/
	      printf("INSERT: Parse failed, status code %d\n", status);
	      rsp_msg->bnodes_str = g_strdup("");
	      rsp_msg->status = status;
	      goto send_response;
	    }
	}


      s->header = header;
      s->req = req_msg;
      s->rsp = rsp_msg;
      s->op_lock = op_lock;
      s->op_cond = op_cond;
      s->op_complete = FALSE;

      g_async_queue_push(param->sib->insert_queue, s);

      /* Signal scheduler that new operation has been added to queue */
      g_mutex_lock(param->sib->new_reqs_lock);
      param->sib->new_reqs = TRUE;
      g_cond_signal(param->sib->new_reqs_cond);
      g_mutex_unlock(param->sib->new_reqs_lock);

      /* Block while operation is being processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);


    /* DAN - ARCES : XXX */
    send_response:
      /* REMOVED rsp_msg->status = status; DAN - ARCES */
      whiteboard_util_send_method_return(param->conn,
					 param->msg,
					 DBUS_TYPE_STRING, &(header->space_id),
					 DBUS_TYPE_STRING, &(header->kp_id),
					 DBUS_TYPE_INT32, &(header->tr_id),
					 DBUS_TYPE_INT32, &(rsp_msg->status),
					 DBUS_TYPE_STRING, &(rsp_msg->bnodes_str),
					 WHITEBOARD_UTIL_LIST_END);

      if (EncodingM3XML == req_msg->encoding)
	{
	  ssFreeTripleList(&(req_msg->insert_graph));
	}
      g_mutex_free(op_lock);
      g_cond_free(op_cond);
      g_free(s);
      g_free(rsp_msg->bnodes_str);
      g_free(req_msg);
      g_free(rsp_msg);
      g_free(header);

    }
  else
    {
      printf("COULD NOT PARSE INSERT DBUS MESSAGE\n");
      whiteboard_log_warning("Could not parse INSERT method call message\n");
    }
  dbus_message_unref(param->msg);
  g_free(param);
  return NULL;

}

gpointer m3_remove(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;
  ssStatus_t status = ss_StatusOK;
  sib_op_parameter* param = (sib_op_parameter*) data;

  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);
  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();
  rsp_msg->status = ss_StatusOK;    /* DAN - ARCES */

  if(whiteboard_util_parse_message(param->msg,
				   DBUS_TYPE_STRING, &(header->space_id),
				   DBUS_TYPE_STRING, &(header->kp_id),
				   DBUS_TYPE_INT32, &(header->tr_id),
				   DBUS_TYPE_INT32, &(req_msg->encoding),
				   DBUS_TYPE_STRING, &(req_msg->remove_str),
				   DBUS_TYPE_INVALID) )
    {
      whiteboard_log_debug("Parsed remove %d\n", header->tr_id);
      /* Initialize message structs */
      header->tr_type = M3_REMOVE;
      header->msg_type = M3_REQUEST;
      if (EncodingM3XML == req_msg->encoding)
	{
	  status = parseM3_triples(&(req_msg->remove_graph),
				   req_msg->remove_str,
				   NULL);
	  if (status != ss_StatusOK)
	    {
	      printf("REMOVE: Parse failed, status code %d\n", status);
	      rsp_msg->status = status; /* DAN - ARCES */
	      goto send_response;
	    }
	}
      s->header = header;
      s->req = req_msg;
      s->rsp = rsp_msg;
      s->op_lock = op_lock;
      s->op_cond = op_cond;
      s->op_complete = FALSE;

      g_async_queue_push(param->sib->insert_queue, s);

      /* Signal scheduler that new operation has been added to queue */
      g_mutex_lock(param->sib->new_reqs_lock);
      param->sib->new_reqs = TRUE;
      g_cond_signal(param->sib->new_reqs_cond);
      g_mutex_unlock(param->sib->new_reqs_lock);

      /* Block while operation is being processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

    send_response:
      /* REMOVED rsp_msg->status = status; DAN - ARCES */
      whiteboard_util_send_method_return(param->conn,
					 param->msg,
					 DBUS_TYPE_STRING, &(header->space_id),
					 DBUS_TYPE_STRING, &(header->kp_id),
					 DBUS_TYPE_INT32, &(header->tr_id),
					 DBUS_TYPE_INT32, &(rsp_msg->status),
					 WHITEBOARD_UTIL_LIST_END);

      ssFreeTripleList(&(req_msg->remove_graph));

      g_mutex_free(op_lock);
      g_cond_free(op_cond);
      g_free(s);
      g_free(req_msg);
      g_free(rsp_msg);
      g_free(header);
    }
  else
    {
      printf("COULD NOT PARSE REMOVE DBUS MESSAGE\n");
      whiteboard_log_warning("Could not parse REMOVE method call message\n");
    }

  dbus_message_unref(param->msg);
  g_free(param);
  return NULL;
}

gpointer m3_update(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;
  ssStatus_t status = ss_StatusOK;
  sib_op_parameter* param = (sib_op_parameter*) data;

  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);
  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();
  rsp_msg->status = ss_StatusOK;    /* DAN - ARCES */

  if(whiteboard_util_parse_message(param->msg,
				   DBUS_TYPE_STRING, &(header->space_id),
				   DBUS_TYPE_STRING, &(header->kp_id),
				   DBUS_TYPE_INT32, &(header->tr_id),
				   DBUS_TYPE_INT32, &(req_msg->encoding),
				   DBUS_TYPE_STRING, &(req_msg->insert_str),
				   DBUS_TYPE_STRING, &(req_msg->remove_str),
				   DBUS_TYPE_INVALID) )
    {
      /* Initialize message structs */
      header->tr_type = M3_UPDATE;
      header->msg_type = M3_REQUEST;
      /* Parse insert and remove strings to lists of triples*/
      if (EncodingM3XML == req_msg->encoding)
	{
	  status = parseM3_triples(&(req_msg->remove_graph),
				   req_msg->remove_str,
				   NULL);
	  if (status != ss_StatusOK)
	    {
	      whiteboard_log_debug("UPDATE: Remove parse failed, status code %d\n", status);
	      rsp_msg->bnodes_str = g_strdup("");
	      rsp_msg->status = status; /* DAN - ARCES */
	      goto send_response;
	    }
	}
      if (EncodingM3XML == req_msg->encoding)
	{
	  status = parseM3_triples_SIB(&(req_msg->insert_graph),
				       req_msg->insert_str,
				       NULL,
				       &(rsp_msg->bnodes_str));
	  if (status != ss_StatusOK)
	    {
	      whiteboard_log_debug("UPDATE: Insert parse failed, status code %d\n", status);
	      rsp_msg->bnodes_str = g_strdup("");
	      rsp_msg->status = status; /* DAN - ARCES */
	      goto send_response;
	    }
	}

      s->header = header;
      s->req = req_msg;
      s->rsp = rsp_msg;
      s->op_lock = op_lock;
      s->op_cond = op_cond;
      s->op_complete = FALSE;

      g_async_queue_push(param->sib->insert_queue, s);

      /* Signal scheduler that new operation has been added to queue */
      g_mutex_lock(param->sib->new_reqs_lock);
      param->sib->new_reqs = TRUE;
      g_cond_signal(param->sib->new_reqs_cond);
      g_mutex_unlock(param->sib->new_reqs_lock);

      /* Block while operation is being processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

    send_response:
    /* REMOVED rsp_msg->status = status; DAN - ARCES */
      whiteboard_util_send_method_return(param->conn,
				  param->msg,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_INT32, &(rsp_msg->status),
				  DBUS_TYPE_STRING, &(rsp_msg->bnodes_str),
				  WHITEBOARD_UTIL_LIST_END);

      ssFreeTripleList(&(req_msg->remove_graph));
      ssFreeTripleList(&(req_msg->insert_graph));
      g_free(s);
      g_free(rsp_msg->bnodes_str);
      g_free(req_msg);
      g_free(rsp_msg);
      g_free(header);
    }
  else
    {
      whiteboard_log_warning("Could not parse UPDATE method call message\n");
    }

  dbus_message_unref(param->msg);
  g_free(param);
  return NULL;

}

#if WITH_WQL == 1
void m3_free_node_int_list(GSList** node_list, GHashTable* current)
{
  if (!node_list || !*node_list)
    return;
  GSList* nl = *node_list;
  m3_node_int* n;
  gint key;
  while (NULL != nl)
    {
      n = nl->data;
      nl = g_slist_remove(nl, n);
      key = n->node;
      if (NULL == current || NULL == g_hash_table_lookup(current, GINT_TO_POINTER(key)))
	{
	  g_free(n->lang);
	  g_free(n);
	}
    }
  *node_list = NULL;
  return;
}
#endif /* WITH_WQL */

void m3_free_triple_int_list(GSList** triple_list, GHashTable* current)
{
  if (!triple_list || !*triple_list)
    return;
  GSList* tl = *triple_list;
  m3_triple_int* t;
  while (NULL != tl)
    {
      t = tl->data;
      //printf("m3_free_triple_int_list: freeing triple %d %d %d\n", t->s, t->p, t->o);
      tl = g_slist_remove(tl, t);
      if (NULL != current)
	{
	  gchar* key = g_strdup_printf("%d_%d_%d", t->s, t->p, t->o);
	  if (NULL == g_hash_table_lookup(current, key))
	    {
	      //printf("m3_free_triple_int_list: HT: triple freed %d %d %d\n", t->s, t->p, t->o);
	      if (t->lang) g_free(t->lang);
	      g_free(t);
	    }
	  //printf("m3_free_triple_int_list: HT: triple not freed %d %d %d\n", t->s, t->p, t->o);
	  g_free(key);
	}
      else
	{
	  printf("m3_free_triple_int_list: !HT: triple freed %d %d %d\n", t->s, t->p, t->o);
	  if (t->lang) g_free(t->lang);
	  g_free(t);
	}
    }
  *triple_list = NULL;
  return;
}

gchar* m3_gen_triple_string(GSList* triples, sib_op_parameter* param)
{
  ssStatus_t status;
  ssBufDesc_t *bd = ssBufDesc_new();
  ssTriple_t *t;
  gchar* retval;

  status = addXML_start(bd, &SIB_TRIPLELIST, NULL, NULL, 0);
  while (status == ss_StatusOK &&
	 NULL != triples)
    {
      t = (ssTriple_t*)triples->data;
      if (NULL == t)
	{
	  status = ss_OperationFailed;
	  printf("m3_gen_triple_string(): triple was NULL\n");
	  goto end;
	}
      status = addXML_templateTriple(t, NULL, (gpointer)bd);
      //printf("Added triple\n%s\n%s\n%s\n to result set\n", t->subject, t->predicate, t->object);
      triples = triples->next;
    }
 end:
  addXML_end(bd, &SIB_TRIPLELIST);
  retval = g_strdup(ssBufDesc_GetMessage(bd));
  ssBufDesc_free(&bd);
  return retval;
}

#if WITH_WQL == 1
gchar* m3_gen_node_string(GSList* nodes, sib_op_parameter* param)
{
  ssStatus_t status;
  ssBufDesc_t *bd = ssBufDesc_new();
  ssPathNode_t *n;
  gchar* retval;

  status = addXML_start(bd, &SIB_NODE_LIST, NULL, NULL, 0);
  while (status == ss_StatusOK &&
	 nodes &&
	 (n = (ssPathNode_t*)nodes->data))
    {
      status = addXML_queryResultNode(bd, n);
      nodes = nodes->next;
    }
  addXML_end(bd, &SIB_NODE_LIST);
  retval = g_strdup(ssBufDesc_GetMessage(bd));
  ssBufDesc_free(&bd);
  return retval;
}
#endif /* WITH_WQL */

GSList* m3_triple_list_int_to_str(GSList* int_triples, DB store, ssStatus_t* status)
{
  GSList* str_triples = NULL;
  m3_triple_int* it;
  ssTriple_t* st;
  char *str_tmp;
  char *str_exp_tmp;

  piglet_transaction(store);
  while (NULL != int_triples)
    {
      st = g_new0(ssTriple_t, 1);
      it = (m3_triple_int*)int_triples->data;
      str_tmp = piglet_info(store, it->s, &(it->dt), it->lang);

      if (!str_tmp)
	goto error;

      str_exp_tmp = piglet_expand_m3(store, str_tmp);
      st->subject = (ssElement_t)g_strdup(str_exp_tmp);
      /* These are allocated in Piglet using malloc */
      free(str_tmp);
      free(str_exp_tmp);

      str_tmp = piglet_info(store, it->p, &(it->dt), it->lang);

      if (!str_tmp)
	goto error;

      str_exp_tmp = piglet_expand_m3(store, str_tmp);
      st->predicate = (ssElement_t)g_strdup(str_exp_tmp);
      /* These are allocated in Piglet using malloc */
      free(str_tmp);
      free(str_exp_tmp);

      if (it->o >= 0)
	{
	  st->objType = ssElement_TYPE_URI;

	  str_tmp = piglet_info(store, it->o, &(it->dt), it->lang);

	  if (!str_tmp)
	    goto error;

	  str_exp_tmp = piglet_expand_m3(store, str_tmp);
	  st->object = (ssElement_t)g_strdup(str_exp_tmp);
	  /* These are allocated in Piglet using malloc */
	  free(str_tmp);
	  free(str_exp_tmp);
	}
      else
	{
	  /* Literals should not be expanded */
	  st->objType = ssElement_TYPE_LIT;
	  str_tmp = piglet_info(store, it->o, &(it->dt), it->lang);

	  if (!str_tmp)
	    goto error;

	  st->object = (ssElement_t)g_strdup(str_tmp);
	  /* This is allocated in Piglet using malloc */
	  free(str_tmp);
	}
        /* dt and lang handling here*/
      str_triples = g_slist_prepend(str_triples, st);
      int_triples = int_triples->next;
    }
  piglet_commit(store);
  return str_triples;
 error:
  /* Cleanup here */
  printf("m3_triple_list_int_to_string: got error:\n%s\n", piglet_error_message);
  piglet_rollback(store);
  ssFreeTripleList(&str_triples);
  ssFreeTriple(st);
  *status = ss_OperationFailed;
  return NULL;

}

#if WITH_WQL==1
GSList* m3_node_list_int_to_str(GSList* int_nodes, DB store, ssStatus_t* status)
{
  GSList* str_nodes = NULL;
  m3_node_int* in;
  ssPathNode_t* sn;
  char* str_tmp_node;
  char* str_exp_tmp_node;

  piglet_transaction(store);
  while (NULL != int_nodes)
    {
      sn = g_new0(ssPathNode_t, 1);
      in = (m3_node_int*)int_nodes->data;
      if (in->node == 0)
	{
	  whiteboard_log_debug("ERROR: Got node value 0!\n");
	  int_nodes = int_nodes->next;
	  continue;
	}
      // whiteboard_log_debug("Got int node %d\n", in->node);
      if (in->node > 0)
	{
	  sn->nodeType = ssElement_TYPE_URI;
	  str_tmp_node = piglet_info(store, in->node, &(in->dt), in->lang);

	  if (!str_tmp_node)
	    goto error;

	  str_exp_tmp_node = piglet_expand_m3(store, str_tmp_node);
	  sn->string = (ssElement_t)g_strdup((gchar*)str_exp_tmp_node);
	  /* These are allocated in Piglet using malloc */
	  free(str_tmp_node);
	  free(str_exp_tmp_node);

	  // whiteboard_log_debug("Int node %d is %s\n", in->node, sn->string);
	}
      else
	{
	  /* Literals should not be expanded */
	  sn->nodeType = ssElement_TYPE_LIT;
	  str_tmp_node = piglet_info(store, in->node, &(in->dt), in->lang);

	  if (!str_tmp_node)
	    goto error;

	  sn->string = (ssElement_t)g_strdup(str_tmp_node);
	  /* This is allocated in Piglet using malloc */
	  free(str_tmp_node);

	  // whiteboard_log_debug("Int node %d is %s\n", in->node, sn->string);
	  /* dt and lang handling here*/
	}
      str_nodes = g_slist_prepend(str_nodes, sn);
      int_nodes = int_nodes->next;
    }
  piglet_commit(store);
  return str_nodes;
 error:
  /* Cleanup here */
  piglet_rollback(store);
  ssFreePathNodeList(&str_nodes);
  ssFreePathNode(sn);
  *status = ss_OperationFailed;
  return NULL;
}
#endif /* WITH_WQL */

gpointer m3_query(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;
  ssStatus_t status;
  sib_op_parameter* param = (sib_op_parameter*) data;

  GSList* res_list_str = NULL;



  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);
  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();

  if(whiteboard_util_parse_message(param->msg,
			    DBUS_TYPE_STRING, &(header->space_id),
			    DBUS_TYPE_STRING, &(header->kp_id),
			    DBUS_TYPE_INT32, &(header->tr_id),
			    DBUS_TYPE_INT32, &(req_msg->type),
			    DBUS_TYPE_STRING, &(req_msg->query_str),
			    DBUS_TYPE_INVALID) )
    {
      /* Initialize message structs */
      header->tr_type = M3_QUERY;
      header->msg_type = M3_REQUEST;
      rsp_msg->status = ss_StatusOK;
      switch(req_msg->type)
	{
	case QueryTypeTemplate:
	  status = parseM3_triples(&(req_msg->template_query),
				   req_msg->query_str,
				   NULL);
	  break;
#if WITH_WQL==1
	case QueryTypeWQLValues:
	case QueryTypeWQLRelated:
	case QueryTypeWQLIsType:
	case QueryTypeWQLIsSubType:
	  req_msg->wql_query = ssWqlDesc_new_jh(req_msg->type);
	  whiteboard_log_debug("WQL query for tr %d is:\n%s\n", header->tr_id, req_msg->query_str);
	  status = parseM3_query_req_wql(req_msg->wql_query,
					 (const gchar*)req_msg->query_str);
	  whiteboard_log_debug("Parsed WQL query for tr %d\n", header->tr_id);

	  break;
	case QueryTypeWQLNodeTypes:
	  /* FALLTHROUGH */
	  /* Not working in current release, will be fixed */
#endif /* WITH_WQL */
	case QueryTypeSPARQLSelect:
	  /* FALLTHROUGH */
	default: /* Error */
	  rsp_msg->status = ss_SIBFailNotImpl;
	  rsp_msg->results_str = g_strdup("");
	  goto send_response;
	  break;
	}

      if (status != ss_StatusOK)
	{
	  if (status == ss_ParsingError)
	    rsp_msg->status = ss_KPErrorMsgSyntax;
	  else
	    rsp_msg->status = status;

	  rsp_msg->results_str = g_strdup("");
	  goto send_response;
	}
      s->header = header;
      s->req = req_msg;
      s->rsp = rsp_msg;
      s->op_lock = op_lock;
      s->op_cond = op_cond;
      s->op_complete = FALSE;

      g_async_queue_push(param->sib->query_queue, s);

      /* Signal scheduler that new operation has been added to queue */
      g_mutex_lock(param->sib->new_reqs_lock);
      param->sib->new_reqs = TRUE;
      g_cond_signal(param->sib->new_reqs_cond);
      g_mutex_unlock(param->sib->new_reqs_lock);

      /* Block while operation is being processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

      /* Generate results strings here */
      switch (req_msg->type)
	{
	case QueryTypeTemplate:
	  g_mutex_lock(param->sib->store_lock);
	  res_list_str = m3_triple_list_int_to_str(rsp_msg->results, param->sib->RDF_store, &status);
	  g_mutex_unlock(param->sib->store_lock);
	  rsp_msg->results_str = m3_gen_triple_string(res_list_str, param);
	  break;
#if WITH_WQL==1
	case QueryTypeWQLNodeTypes:
	  /* FALLTHROUGH */
	case QueryTypeWQLValues:
	  g_mutex_lock(param->sib->store_lock);
	  res_list_str = m3_node_list_int_to_str(rsp_msg->results, param->sib->RDF_store, &status);
	  g_mutex_unlock(param->sib->store_lock);
	  rsp_msg->results_str = m3_gen_node_string(res_list_str, param);
	  whiteboard_log_debug("Generated results string %s\n", rsp_msg->results_str);
	  break;
	case QueryTypeWQLRelated:
	  /* FALLTHROUGH */
	case QueryTypeWQLIsType:
	  /* FALLTHROUGH */
	case QueryTypeWQLIsSubType:
	  if (rsp_msg->bool_results)
	    rsp_msg->results_str = g_strdup("TRUE");
	  else
	    rsp_msg->results_str = g_strdup("FALSE");
	  break;
#endif /* WITH_WQL */
	case QueryTypeSPARQLSelect:
	  break;
	default: /* Error */
	  /* Should never be reached */
	  /* assert(0); */
	  break;
	}
    send_response:
      whiteboard_util_send_method_return(param->conn,
				  param->msg,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_INT32, &(rsp_msg->status),
				  DBUS_TYPE_STRING, &(rsp_msg->results_str),
				  WHITEBOARD_UTIL_LIST_END);
      /* Free memory*/
      switch (req_msg->type)
	{
	case QueryTypeTemplate:
	  ssFreeTripleList(&res_list_str);
	  ssFreeTripleList(&(req_msg->template_query));
	  m3_free_triple_int_list(&(rsp_msg->results), NULL);
	  break;
#if WITH_WQL==1
	case QueryTypeWQLValues:
	  /* FALLTHROUGH */
	  ssFreePathNodeList(&res_list_str);
	  ssWqlDesc_free(&(req_msg->wql_query));
	  m3_free_node_int_list(&(rsp_msg->results), NULL);
	  break;
	case QueryTypeWQLRelated:
	  /* FALLTHROUGH */
	case QueryTypeWQLIsType:
	  /* FALLTHROUGH */
	case QueryTypeWQLIsSubType:
	  ssWqlDesc_free(&req_msg->wql_query);
	  break;
	case QueryTypeWQLNodeTypes:
	  /* FALLTHROUGH */
	  /* Not working in current release, will be fixed */
#endif /* WITH_WQL */
	case QueryTypeSPARQLSelect:
	  /* FALLTHROUGH */
	default: /* Error */
	  /* Should not ever be reached */
	  /* assert(0); */
	  break;
	}
      g_mutex_free(op_lock);
      g_cond_free(op_cond);
      g_free(s);
      g_free(rsp_msg->results_str);
      g_free(req_msg);
      g_free(rsp_msg);
      g_free(header);
    }
  else
    {
      whiteboard_log_warning("Could not parse QUERY method call message\n");
    }

  dbus_message_unref(param->msg);
  g_free(param);
  return NULL;

}

gboolean m3_sub_free_int_triple(gpointer key, gpointer value, gpointer not_used)
{
  g_free(value);
  return TRUE;
}

#if WITH_WQL==1
gboolean m3_sub_free_int_node(gpointer key, gpointer value, gpointer not_used)
{
  g_free(value);
  return TRUE;
}
#endif /* WITH_WQL */

GHashTable* m3_sub_result_init_triples(GSList* baseline)
{
  GHashTable* hash_baseline = g_hash_table_new_full(g_str_hash, g_str_equal,
						    g_free, NULL);

  gchar* key;
  m3_triple_int* t;

  for ( ; baseline != NULL ; baseline = g_slist_next(baseline))
    {
      /* Store triple from new result to hash */
      t = (m3_triple_int*)baseline->data;
      key = g_strdup_printf("%d_%d_%d", t->s, t->p, t->o);
      g_hash_table_insert(hash_baseline, key, t);
    }
  return hash_baseline;
}

GHashTable* m3_sub_diff_triples(GHashTable* previous, GSList* new_result,
				GSList** added, GSList** removed)
{
  /*
   * Function to generate a diff from new results and previous results
   * Returns a hash table containing new results.
   * The added and removed items are returned in added and removed
   * parameters
   *
   * previous: a GHashTable of m3_triple_int with key of string "s_p_o"
   * new_result: a GSList of m3_triple_int
   *
   */

  GHashTableIter iter;
  /* Contains m3_triple_int */
  GHashTable* new_hash = g_hash_table_new_full(g_str_hash, g_str_equal,
					       g_free, NULL);
  gchar* key;
  m3_triple_int *t, *tmp;

  /* Contains m3_triple_int */
  GSList* added_tmp = *added;
  /* Contains m3_triple_int */
  GSList* removed_tmp = *removed;
  for ( ; new_result != NULL ; new_result = new_result->next)
    {
      /* Store triple from new result to hash */
      t = (m3_triple_int*)new_result->data;
      key = g_strdup_printf("%d_%d_%d", t->s, t->p, t->o);
      g_hash_table_insert(new_hash, key, t);
      tmp = g_hash_table_lookup(previous, key);
      if (NULL != tmp)
	{
	  g_hash_table_remove(previous, key);
	  g_free(tmp);
	}
      else
	{
	  added_tmp = g_slist_prepend(added_tmp, t);
	}
    }
  g_hash_table_iter_init(&iter, previous);

  while(g_hash_table_iter_next(&iter, (gpointer*)&key, (gpointer*)&t))
    {
      removed_tmp = g_slist_prepend(removed_tmp, t);
    }
  *removed = removed_tmp;
  *added = added_tmp;
  g_hash_table_destroy(previous);
  return new_hash;
}

#if WITH_WQL==1
GHashTable* m3_sub_result_init_nodes(GSList* baseline)
{
  GHashTable* hash_baseline = g_hash_table_new_full(g_direct_hash, g_direct_equal,
						    NULL, NULL);
  m3_node_int* n;
  gint key;
  for ( ; baseline != NULL ; baseline = g_slist_next(baseline))
    {
      /* Store triple from new result to hash */
      n = (m3_node_int*)baseline->data;
      key = n->node;
      g_hash_table_insert(hash_baseline, GINT_TO_POINTER(key), n);
    }
  return hash_baseline;
}

GHashTable* m3_sub_diff_nodes(GHashTable* previous, GSList* new_result,
				GSList** added, GSList** removed)
{
  /*
   * Function to generate a diff from new results and previous results
   * Returns a hash table containing new results.
   * The added and removed items are returned in added and removed
   * parameters
   */

  GHashTableIter iter;
  GHashTable* new_hash = g_hash_table_new_full(g_direct_hash, g_direct_equal,
					       NULL, NULL);
  m3_node_int* n;
  m3_node_int* tmpData;
  GSList* added_tmp = *added;
  GSList* removed_tmp = *removed;
  gint key;

  for ( ; new_result != NULL ; new_result = g_slist_next(new_result))
    {
      /* Store triple from new result to hash */
      n = (m3_node_int*)new_result->data;
      key = n->node;
      g_hash_table_insert(new_hash, GINT_TO_POINTER(key), n);
      tmpData = g_hash_table_lookup(previous, GINT_TO_POINTER(key));
      if (NULL != tmpData)
	{
	  g_free(tmpData);
	  g_hash_table_remove(previous, GINT_TO_POINTER(key));
	}
      else
	{
	  added_tmp = g_slist_prepend(added_tmp, n);
	}

    }

  g_hash_table_iter_init(&iter, previous);

  while(g_hash_table_iter_next(&iter, NULL, (gpointer *)&tmpData))
    {
      removed_tmp = g_slist_prepend(removed_tmp, tmpData);
    }

  *removed = removed_tmp;
  *added = added_tmp;
  g_hash_table_destroy(previous);
  return new_hash;
}
#endif /* WITH_WQL */

gint m3_subscribe_triples(ssap_message_header *header,
			  ssap_kp_message *req_msg,
			  ssap_sib_message *rsp_msg,
			  sib_op_parameter* param)
{
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;

  gchar *space_id, *kp_id;
  gchar* sub_id = rsp_msg->sub_id;
  gint tr_id;

  GHashTable* current_result;
  GSList *added = NULL, *removed = NULL, *added_str = NULL, *removed_str = NULL;

  subscription_state *sub_state;


  space_id = header->space_id;
  kp_id = header->kp_id;
  tr_id = header->tr_id;

  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();

  s->header = header;
  s->req = req_msg;
  s->rsp = rsp_msg;
  s->op_lock = op_lock;
  s->op_cond = op_cond;
  s->op_complete = FALSE;


  g_async_queue_push(param->sib->query_queue, s);

  /* Signal scheduler that new operation has been added to queue */
  g_mutex_lock(param->sib->new_reqs_lock);
  param->sib->new_reqs = TRUE;
  // printf("Now signaling SCHEDULER in SUBSCRIBE tr %d\n", header->tr_id);
  g_cond_signal(param->sib->new_reqs_cond);
  g_mutex_unlock(param->sib->new_reqs_lock);

  /* Block while operation is being processed */
  g_mutex_lock(s->op_lock);
  while (!(s->op_complete))
    {
      g_cond_wait(s->op_cond, s->op_lock);
    }
  s->op_complete = FALSE;
  g_mutex_unlock(op_lock);
  printf("Got baseline query result for subscription %s\n", rsp_msg->sub_id); /* SUB_DEBUG */

  current_result = m3_sub_result_init_triples(rsp_msg->results);

  g_mutex_lock(param->sib->store_lock);
  added_str = m3_triple_list_int_to_str(rsp_msg->results, param->sib->RDF_store, &(rsp_msg->status));
  g_mutex_unlock(param->sib->store_lock);

  rsp_msg->results_str = m3_gen_triple_string(added_str, param);

  whiteboard_util_send_method_return(param->conn,
				     param->msg,
				     DBUS_TYPE_STRING, &(space_id),
				     DBUS_TYPE_STRING, &(kp_id),
				     DBUS_TYPE_INT32, &(tr_id),
				     DBUS_TYPE_INT32, &(rsp_msg->status),
				     DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				     DBUS_TYPE_STRING, &(rsp_msg->results_str),
				     WHITEBOARD_UTIL_LIST_END);

  m3_free_triple_int_list(&(rsp_msg->results), current_result);
  g_free(rsp_msg->results_str);
  dbus_message_unref(param->msg);
  ssFreeTripleList(&added_str);

  do
    {

      /* Check if unsubscribed */
      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_STOPPED)
	{
	  /* LOCK UNSUB LOCK */
	  // g_mutex_lock(sub_state->unsub_lock);
	  /* LOCK SUBSCRIPTION TABLE */
	  g_mutex_lock(param->sib->subscriptions_lock);
	  g_free(sub_state->sub_id);
	  g_free(sub_state->kp_id);
	  g_hash_table_remove(param->sib->subs, sub_id);
	  sub_state->unsub = TRUE;
	  g_cond_signal(sub_state->unsub_cond);
	  /* UNLOCK SUBSCRIPTION TABLE */
	  g_mutex_unlock(param->sib->subscriptions_lock);
	  /* UNLOCK UNSUB LOCK */
	  // g_mutex_unlock(sub_state->unsub_lock);
	  g_hash_table_foreach_remove(current_result, m3_sub_free_int_triple, NULL);
	  m3_free_triple_int_list(&(rsp_msg->results), NULL);
	  break;
	}

      g_async_queue_push(param->sib->query_queue, s);

      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_PENDING)
	{
	  g_mutex_lock(param->sib->new_reqs_lock);
	  param->sib->new_reqs = TRUE;
	  g_cond_signal(param->sib->new_reqs_cond);
	  g_mutex_unlock(param->sib->new_reqs_lock);
	  printf("Subscription %s pending, setting new_reqs flag\n", rsp_msg->sub_id); /* SUB_DEBUG */
	}

      /* Block while operation is processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

      whiteboard_log_debug("Got new subscription result for transaction %d\n", tr_id);
      printf("Got new query result for subscription %s\n", rsp_msg->sub_id); /* SUB_DEBUG */

      current_result = m3_sub_diff_triples(current_result, rsp_msg->results, &added, &removed);


      if (added == NULL && removed == NULL)
	{
	  m3_free_triple_int_list(&(rsp_msg->results), current_result);
	  printf("New result for subscription %s was not changed\n", rsp_msg->sub_id); /* SUB_DEBUG */
	  whiteboard_log_debug("Subscription result was not changed for transaction %d\n", tr_id);
	  continue;
	}

      g_mutex_lock(param->sib->store_lock);
      added_str = m3_triple_list_int_to_str(added,
					    param->sib->RDF_store,
					    &(rsp_msg->status));
      g_mutex_unlock(param->sib->store_lock);
      rsp_msg->new_results_str = m3_gen_triple_string(added_str, param);

      g_mutex_lock(param->sib->store_lock);
      removed_str = m3_triple_list_int_to_str(removed,
					      param->sib->RDF_store,
					      &(rsp_msg->status));
      g_mutex_unlock(param->sib->store_lock);
      rsp_msg->obsolete_results_str = m3_gen_triple_string(removed_str, param);

      if( ++(rsp_msg->ind_seqnum) == SSAP_IND_WRAP_NUM )
	rsp_msg->ind_seqnum=1;

      whiteboard_util_send_signal(SIB_DBUS_OBJECT,
				  SIB_DBUS_KP_INTERFACE,
				  SIB_DBUS_KP_SIGNAL_SUBSCRIPTION_IND,
				  param->conn,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_INT32, &(rsp_msg->ind_seqnum),
				  DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				  DBUS_TYPE_STRING, &(rsp_msg->new_results_str),
				  DBUS_TYPE_STRING, &(rsp_msg->obsolete_results_str),
				  WHITEBOARD_UTIL_LIST_END);

      printf("Sent new result for sub %s, seqnum %d to transport\n",
	     rsp_msg->sub_id, rsp_msg->ind_seqnum); /* SUB_DEBUG */
      /* Free memory */
      ssFreeTripleList(&added_str);
      ssFreeTripleList(&removed_str);

      m3_free_triple_int_list(&added, current_result);
      m3_free_triple_int_list(&removed, current_result);
      m3_free_triple_int_list(&(rsp_msg->results), current_result);

      whiteboard_log_debug("Subscription result for transaction %d is\n%s\n%s\n", tr_id,
			   rsp_msg->new_results_str,
			   rsp_msg->obsolete_results_str);

      g_free(rsp_msg->new_results_str);
      g_free(rsp_msg->obsolete_results_str);
    }

  while(TRUE);
  g_cond_free(op_cond);
  g_mutex_free(op_lock);
  g_free(s);
  return ss_StatusOK;
}


gint m3_subscribe_SPARQL(ssap_message_header *header,
			 ssap_kp_message *req_msg,
			 ssap_sib_message *rsp_msg,
			 sib_op_parameter* param)
{
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;

  gchar *space_id, *kp_id;
  gchar* sub_id = rsp_msg->sub_id;
  gint tr_id;

  GHashTable* current_result;
  GSList *added = NULL, *removed = NULL, *added_str = NULL, *removed_str = NULL;

  subscription_state *sub_state;

  space_id = header->space_id;
  kp_id = header->kp_id;
  tr_id = header->tr_id;

  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();

  s->header = header;
  s->req = req_msg;
  s->rsp = rsp_msg;
  s->op_lock = op_lock;
  s->op_cond = op_cond;
  s->op_complete = FALSE;


  g_async_queue_push(param->sib->query_queue, s);

  /* Signal scheduler that new operation has been added to queue */
  g_mutex_lock(param->sib->new_reqs_lock);
  param->sib->new_reqs = TRUE;
  // printf("Now signaling SCHEDULER in SUBSCRIBE tr %d\n", header->tr_id);
  g_cond_signal(param->sib->new_reqs_cond);
  g_mutex_unlock(param->sib->new_reqs_lock);

  /* Block while operation is being processed */
  g_mutex_lock(s->op_lock);
  while (!(s->op_complete))
    {
      g_cond_wait(s->op_cond, s->op_lock);
    }
  s->op_complete = FALSE;
  g_mutex_unlock(op_lock);

  current_result = m3_sub_result_init_triples(rsp_msg->results);

  g_mutex_lock(param->sib->store_lock);
  added_str = m3_triple_list_int_to_str(rsp_msg->results, param->sib->RDF_store, &(rsp_msg->status));
  g_mutex_unlock(param->sib->store_lock);

  rsp_msg->results_str = m3_gen_triple_string(added_str, param);

  whiteboard_util_send_method_return(param->conn,
				     param->msg,
				     DBUS_TYPE_STRING, &(space_id),
				     DBUS_TYPE_STRING, &(kp_id),
				     DBUS_TYPE_INT32, &(tr_id),
				     DBUS_TYPE_INT32, &(rsp_msg->status),
				     DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				     DBUS_TYPE_STRING, &(rsp_msg->results_str),
				     WHITEBOARD_UTIL_LIST_END);

  m3_free_triple_int_list(&(rsp_msg->results), current_result);
  g_free(rsp_msg->results_str);
  dbus_message_unref(param->msg);
  ssFreeTripleList(&added_str);

  do
    {

      /* Check if unsubscribed */
      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_STOPPED)
	{
	  /* LOCK SUBSCRIPTION TABLE */
	  g_mutex_lock(param->sib->subscriptions_lock);
	  g_free(sub_state->sub_id);
	  g_free(sub_state->kp_id);
	  g_hash_table_remove(param->sib->subs, sub_id);
	  sub_state->unsub = TRUE;
	  g_cond_signal(sub_state->unsub_cond);
	  /* UNLOCK SUBSCRIPTION TABLE */
	  g_mutex_unlock(param->sib->subscriptions_lock);
	  g_hash_table_foreach_remove(current_result, m3_sub_free_int_triple, NULL);
	  m3_free_triple_int_list(&(rsp_msg->results), NULL);
	  break;
	}

      g_async_queue_push(param->sib->query_queue, s);

      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_PENDING)
	{
	  g_mutex_lock(param->sib->new_reqs_lock);
	  param->sib->new_reqs = TRUE;
	  g_cond_signal(param->sib->new_reqs_cond);
	  g_mutex_unlock(param->sib->new_reqs_lock);
	}

      /* Block while operation is processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

      whiteboard_log_debug("Got new subscription result for transaction %d\n", tr_id);

      current_result = m3_sub_diff_triples(current_result, rsp_msg->results, &added, &removed);


      if (added == NULL && removed == NULL)
	{
	  m3_free_triple_int_list(&(rsp_msg->results), current_result);
	  whiteboard_log_debug("Subscription result was not changed for transaction %d\n", tr_id);
	  continue;
	}

      g_mutex_lock(param->sib->store_lock);
      added_str = m3_triple_list_int_to_str(added,
					    param->sib->RDF_store,
					    &(rsp_msg->status));
      g_mutex_unlock(param->sib->store_lock);
      rsp_msg->new_results_str = m3_gen_triple_string(added_str, param);

      g_mutex_lock(param->sib->store_lock);
      removed_str = m3_triple_list_int_to_str(removed,
					      param->sib->RDF_store,
					      &(rsp_msg->status));
      g_mutex_unlock(param->sib->store_lock);
      rsp_msg->obsolete_results_str = m3_gen_triple_string(removed_str, param);

      whiteboard_util_send_signal(SIB_DBUS_OBJECT,
				  SIB_DBUS_KP_INTERFACE,
				  SIB_DBUS_KP_SIGNAL_SUBSCRIPTION_IND,
				  param->conn,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				  DBUS_TYPE_STRING, &(rsp_msg->new_results_str),
				  DBUS_TYPE_STRING, &(rsp_msg->obsolete_results_str),
				  WHITEBOARD_UTIL_LIST_END);

      /* Free memory */
      ssFreeTripleList(&added_str);
      ssFreeTripleList(&removed_str);

      m3_free_triple_int_list(&added, current_result);
      m3_free_triple_int_list(&removed, current_result);
      m3_free_triple_int_list(&(rsp_msg->results), current_result);

      whiteboard_log_debug("Subscription result for transaction %d is\n%s\n%s\n", tr_id,
			   rsp_msg->new_results_str,
			   rsp_msg->obsolete_results_str);

      g_free(rsp_msg->new_results_str);
      g_free(rsp_msg->obsolete_results_str);
    }

  while(TRUE);
  g_cond_free(op_cond);
  g_mutex_free(op_lock);
  g_free(s);
  return ss_StatusOK;
}

#if WITH_WQL==1
gint m3_subscribe_nodes(ssap_message_header *header,
			ssap_kp_message *req_msg,
			ssap_sib_message *rsp_msg,
			sib_op_parameter* param)
{
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;

  gchar *space_id;
  gchar *kp_id;
  gchar* sub_id = rsp_msg->sub_id;
  gint tr_id;

  GHashTable* current_result;
  GSList *added = NULL;
  GSList *removed = NULL;
  GSList *added_str = NULL;
  GSList *removed_str = NULL;
  subscription_state* sub_state;

  space_id = header->space_id;
  kp_id = header->kp_id;
  tr_id = header->tr_id;

  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();

  s->header = header;
  s->req = req_msg;
  s->rsp = rsp_msg;
  s->op_lock = op_lock;
  s->op_cond = op_cond;
  s->op_complete = FALSE;

  g_async_queue_push(param->sib->query_queue, s);

  /* Signal scheduler that new operation has been added to queue */
  g_mutex_lock(param->sib->new_reqs_lock);
  param->sib->new_reqs = TRUE;
  g_cond_signal(param->sib->new_reqs_cond);
  g_mutex_unlock(param->sib->new_reqs_lock);


  /* Block while operation is processed */
  g_mutex_lock(s->op_lock);
  while (!(s->op_complete))
    {
      g_cond_wait(s->op_cond, s->op_lock);
    }
  s->op_complete = FALSE;
  g_mutex_unlock(op_lock);

  current_result = m3_sub_result_init_nodes(rsp_msg->results);

  g_mutex_lock(param->sib->store_lock);
  added_str = m3_node_list_int_to_str(rsp_msg->results, param->sib->RDF_store, &(rsp_msg->status));
  g_mutex_unlock(param->sib->store_lock);

  rsp_msg->results_str = m3_gen_node_string(added_str, param);

  whiteboard_util_send_method_return(param->conn,
				     param->msg,
				     DBUS_TYPE_STRING, &(space_id),
				     DBUS_TYPE_STRING, &(kp_id),
				     DBUS_TYPE_INT32, &(tr_id),
				     DBUS_TYPE_INT32, &(rsp_msg->status),
				     DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				     DBUS_TYPE_STRING, &(rsp_msg->results_str),
				     WHITEBOARD_UTIL_LIST_END);

  dbus_message_unref(param->msg);
  g_free(rsp_msg->results_str);
  ssFreePathNodeList(&added_str);

  do
    {
      /* Check if unsubscribed */
      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_STOPPED)
	{
	  // g_mutex_lock(sub_state->unsub_lock);
	  g_mutex_lock(param->sib->subscriptions_lock);
	  g_free(sub_state->sub_id);
	  g_free(sub_state->kp_id);
	  g_hash_table_remove(param->sib->subs, sub_id);
	  sub_state->unsub = TRUE;
	  g_cond_signal(sub_state->unsub_cond);
	  g_mutex_unlock(param->sib->subscriptions_lock);
	  // g_mutex_unlock(sub_state->unsub_lock);
	  g_hash_table_foreach_remove(current_result, m3_sub_free_int_node, NULL);
	  m3_free_node_int_list(&(rsp_msg->results), NULL);
	  break;
	}

      g_async_queue_push(param->sib->query_queue, s);

      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_PENDING)
	{
	  g_mutex_lock(param->sib->new_reqs_lock);
	  param->sib->new_reqs = TRUE;
	  g_cond_signal(param->sib->new_reqs_cond);
	  g_mutex_unlock(param->sib->new_reqs_lock);
	}

      /* Block while operation is processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

      current_result = m3_sub_diff_nodes(current_result, rsp_msg->results, &added, &removed);

      if (added == NULL && removed == NULL)
	{
	  m3_free_node_int_list(&(rsp_msg->results), current_result);
	  continue;
	}

      g_mutex_lock(param->sib->store_lock);
      added_str = m3_node_list_int_to_str(added,
					  param->sib->RDF_store,
					  &(rsp_msg->status));
      g_mutex_unlock(param->sib->store_lock);
      rsp_msg->new_results_str = m3_gen_node_string(added_str, param);

      g_mutex_lock(param->sib->store_lock);
      removed_str = m3_node_list_int_to_str(removed,
					    param->sib->RDF_store,
					    &(rsp_msg->status));
      g_mutex_unlock(param->sib->store_lock);
      rsp_msg->obsolete_results_str = m3_gen_node_string(removed_str, param);
      if( ++(rsp_msg->ind_seqnum) == SSAP_IND_WRAP_NUM )
	    rsp_msg->ind_seqnum=1;

      whiteboard_util_send_signal(SIB_DBUS_OBJECT,
				  SIB_DBUS_KP_INTERFACE,
				  SIB_DBUS_KP_SIGNAL_SUBSCRIPTION_IND,
				  param->conn,
				  DBUS_TYPE_STRING, &(header->space_id),
				  DBUS_TYPE_STRING, &(header->kp_id),
				  DBUS_TYPE_INT32, &(header->tr_id),
				  DBUS_TYPE_INT32, &(rsp_msg->ind_seqnum),
				  DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				  DBUS_TYPE_STRING, &(rsp_msg->new_results_str),
				  DBUS_TYPE_STRING, &(rsp_msg->obsolete_results_str),
				  WHITEBOARD_UTIL_LIST_END);

      ssFreePathNodeList(&added_str);
      ssFreePathNodeList(&removed_str);

      m3_free_node_int_list(&added, current_result);
      m3_free_node_int_list(&removed, current_result);
      m3_free_node_int_list(&(rsp_msg->results), current_result);

      g_free(rsp_msg->new_results_str);
      g_free(rsp_msg->obsolete_results_str);

    }
  while(TRUE);


  g_mutex_free(op_lock);
  g_cond_free(op_cond);

  g_free(s);
  return ss_StatusOK;
}

gint m3_subscribe_bool(ssap_message_header *header,
		       ssap_kp_message *req_msg,
		       ssap_sib_message *rsp_msg,
		       sib_op_parameter* param)
{
  GMutex* op_lock;
  GCond* op_cond;
  scheduler_item* s;

  gchar *space_id, *kp_id;
  gchar* sub_id = rsp_msg->sub_id;
  gint tr_id;

  gint current_result_bool;
  /* gint new_result_bool, added_bool, removed_bool;*/

  subscription_state* sub_state;

  space_id = header->space_id;
  kp_id = header->kp_id;
  tr_id = header->tr_id;

  s = g_new0(scheduler_item, 1);
  op_lock = g_mutex_new();
  op_cond = g_cond_new();

  s->header = header;
  s->req = req_msg;
  s->rsp = rsp_msg;
  s->op_lock = op_lock;
  s->op_cond = op_cond;
  s->op_complete = FALSE;

  g_async_queue_push(param->sib->query_queue, s);

  /* Signal scheduler that new operation has been added to queue */
  g_mutex_lock(param->sib->new_reqs_lock);
  param->sib->new_reqs = TRUE;
  g_cond_signal(param->sib->new_reqs_cond);
  g_mutex_unlock(param->sib->new_reqs_lock);

  /* Block while operation is being processed */
  g_mutex_lock(s->op_lock);
  while (!(s->op_complete))
    {
      g_cond_wait(s->op_cond, s->op_lock);
    }
  s->op_complete = FALSE;
  g_mutex_unlock(op_lock);

  current_result_bool = rsp_msg->bool_results;

  if (rsp_msg->bool_results)
    rsp_msg->results_str = g_strdup("TRUE");
  else
    rsp_msg->results_str = g_strdup("FALSE");

  whiteboard_util_send_method_return(param->conn,
				     param->msg,
				     DBUS_TYPE_STRING, &(space_id),
				     DBUS_TYPE_STRING, &(kp_id),
				     DBUS_TYPE_INT32, &(tr_id),
				     DBUS_TYPE_INT32, &(rsp_msg->status),
				     DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				     DBUS_TYPE_STRING, &(rsp_msg->results_str),
				     WHITEBOARD_UTIL_LIST_END);
  dbus_message_unref(param->msg);
  g_free(rsp_msg->results_str);


  do
    {

      /* Check if unsubscribed */
      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_STOPPED)
	{
	  g_mutex_lock(param->sib->subscriptions_lock);
	  g_free(sub_state->sub_id);
	  g_free(sub_state->kp_id);
	  g_hash_table_remove(param->sib->subs, sub_id);
	  sub_state->unsub = TRUE;
	  g_cond_signal(sub_state->unsub_cond);
	  g_mutex_unlock(param->sib->subscriptions_lock);
	  break;
	}

      g_async_queue_push(param->sib->query_queue, s);

      g_mutex_lock(param->sib->subscriptions_lock);
      sub_state = (subscription_state*)g_hash_table_lookup(param->sib->subs, sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (sub_state->status == M3_SUB_PENDING)
	{
	  g_mutex_lock(param->sib->new_reqs_lock);
	  param->sib->new_reqs = TRUE;
	  g_cond_signal(param->sib->new_reqs_cond);
	  g_mutex_unlock(param->sib->new_reqs_lock);
	}

      /* Block while operation is processed */
      g_mutex_lock(s->op_lock);
      while (!(s->op_complete))
	{
	  g_cond_wait(s->op_cond, s->op_lock);
	}
      s->op_complete = FALSE;
      g_mutex_unlock(op_lock);

      if (current_result_bool != rsp_msg->bool_results)
	{
	  switch(rsp_msg->bool_results)
	    {
	    case true:
	      rsp_msg->new_results_str = g_strdup("TRUE");
	      rsp_msg->obsolete_results_str = g_strdup("FALSE");
	      break;
	    case false:
	      rsp_msg->new_results_str = g_strdup("FALSE");
	      rsp_msg->obsolete_results_str = g_strdup("TRUE");
	      break;
	    }

	  if( ++(rsp_msg->ind_seqnum) == SSAP_IND_WRAP_NUM )
	    rsp_msg->ind_seqnum=1;

	  whiteboard_util_send_signal(SIB_DBUS_OBJECT,
				      SIB_DBUS_KP_INTERFACE,
				      SIB_DBUS_KP_SIGNAL_SUBSCRIPTION_IND,
				      param->conn,
				      DBUS_TYPE_STRING, &(header->space_id),
				      DBUS_TYPE_STRING, &(header->kp_id),
				      DBUS_TYPE_INT32, &(header->tr_id),
				      DBUS_TYPE_INT32, &(rsp_msg->ind_seqnum),
				      DBUS_TYPE_STRING, &(rsp_msg->sub_id),
				      DBUS_TYPE_STRING, &(rsp_msg->new_results_str),
				      DBUS_TYPE_STRING, &(rsp_msg->obsolete_results_str),
				      WHITEBOARD_UTIL_LIST_END);

	  current_result_bool = rsp_msg->bool_results;
	  g_free(rsp_msg->new_results_str);
	  g_free(rsp_msg->obsolete_results_str);
	}
    }
  while(TRUE);

  g_mutex_free(op_lock);
  g_cond_free(op_cond);
  g_free(s);
  return ss_StatusOK;
}
#endif /* WITH_WQL */

gpointer m3_subscribe(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;

  ssStatus_t status;
  sib_op_parameter* param = (sib_op_parameter*) data;

  gchar *space_id, *kp_id;
  gchar *temp_sub_id = NULL;
  gint tr_id;
  member_data* kp_data;
  subscription_state* sub_state;

  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);

  /* Initialize message structs */
  header->tr_type = M3_SUBSCRIBE;
  header->msg_type = M3_REQUEST;

  sub_state = g_new0(subscription_state, 1);

  if(whiteboard_util_parse_message(param->msg,
			    DBUS_TYPE_STRING, &space_id,
			    DBUS_TYPE_STRING, &kp_id,
			    DBUS_TYPE_INT32, &tr_id,
			    DBUS_TYPE_INT32, &(req_msg->type),
			    DBUS_TYPE_STRING, &(req_msg->query_str),
			    DBUS_TYPE_INVALID) )
    {
      header->space_id = g_strdup(space_id);
      header->kp_id = g_strdup(kp_id);
      header->tr_id = tr_id;

      /* ASSIGN SUB ID HERE */
      temp_sub_id = g_strdup_printf("%s_%d", kp_id, tr_id);
      g_mutex_lock(param->sib->subscriptions_lock);
      while (g_hash_table_lookup(param->sib->subs, temp_sub_id)) {
	/* Create better sub_id generator! */
	g_free(temp_sub_id);
	temp_sub_id = g_strdup_printf("%s_%d", kp_id, ++tr_id);
      }
      sub_state->status = M3_SUB_ONGOING;
      sub_state->sub_id = g_strdup(temp_sub_id);
      sub_state->kp_id = g_strdup(kp_id);

      g_hash_table_insert(param->sib->subs, (gpointer)temp_sub_id, (gpointer)sub_state);
      g_mutex_unlock(param->sib->subscriptions_lock);

      g_mutex_lock(param->sib->members_lock);
      kp_data = g_hash_table_lookup(param->sib->joined, kp_id);
      if (kp_data != NULL)
	{
	  kp_data->subs = g_slist_prepend(kp_data->subs, g_strdup(temp_sub_id));
	  kp_data->n_of_subs++;
	}
      g_mutex_unlock(param->sib->members_lock);

      rsp_msg->sub_id = g_strdup(temp_sub_id);
      /* Initialize the query structure and start
	 suitable subscription processor */
      switch(req_msg->type)
	{
	case QueryTypeTemplate:
	  status = parseM3_triples(&(req_msg->template_query),
				   req_msg->query_str,
				   NULL);
	  if (status == ss_StatusOK)
	    {
	      printf("Started triples subscription with id %s", rsp_msg->sub_id); /* SUB_DEBUG */
	      status = m3_subscribe_triples(header, req_msg, rsp_msg, param);
	    }
	  else
	    break;
	  break;
#if WITH_WQL==1
	case QueryTypeWQLValues:
	  req_msg->wql_query = ssWqlDesc_new_jh(req_msg->type);
	  status = parseM3_query_req_wql(req_msg->wql_query,
					 (const gchar*)req_msg->query_str);
	  if (status == ss_StatusOK)
	    {
	      printf("Started WQL nodes subscription with id %s", rsp_msg->sub_id); /* SUB_DEBUG */
	      status = m3_subscribe_nodes(header, req_msg, rsp_msg, param);
	    }
	  else
	    break;
	  break;
	case QueryTypeWQLRelated:
	case QueryTypeWQLIsType:
	case QueryTypeWQLIsSubType:
	  req_msg->wql_query = ssWqlDesc_new_jh(req_msg->type);
	  status = parseM3_query_req_wql(req_msg->wql_query,
					 (const gchar*)req_msg->query_str);
	  if (status == ss_StatusOK)
	    {
	      printf("Started WQL boolean subscription with id %s", rsp_msg->sub_id); /* SUB_DEBUG */
	      status = m3_subscribe_bool(header, req_msg, rsp_msg, param);
	    }
	  else
	    break;
	  break;
	case QueryTypeWQLNodeTypes:
	  /* FALLTHROUGH */
	  /* Not working in current release, will be fixed */
#endif /* WITH_WQL */
	default: /* Error */
	  status = ss_SIBFailNotImpl;
	  break;
	}
      printf("SUBSCRIBE: subscription %s finished \n", rsp_msg->sub_id);
    }
  else
    {
      whiteboard_log_warning("Could not parse SUBSCRIBE method call message\n");
    }
  g_free(temp_sub_id);
  g_free(req_msg);
  g_free(rsp_msg->sub_id);
  g_free(rsp_msg);
  g_free(header->space_id);
  g_free(header->kp_id);
  g_free(header);
  g_free(sub_state);
  g_free(param);
  return NULL;

}

gpointer m3_unsubscribe(gpointer data)
{
  ssap_message_header *header;
  ssap_kp_message *req_msg;
  ssap_sib_message *rsp_msg;
  sib_op_parameter* param = (sib_op_parameter*) data;
  //GMutex* unsub_lock;
  GCond* unsub_cond;
  subscription_state* sub;

  //unsub_lock = g_mutex_new();
  unsub_cond = g_cond_new();
  /* Allocate memory for message structs */
  header =  g_new0(ssap_message_header, 1);
  req_msg = g_new0(ssap_kp_message, 1);
  rsp_msg = g_new0(ssap_sib_message, 1);

  if(whiteboard_util_parse_message(param->msg,
			    DBUS_TYPE_STRING, &(header->space_id),
			    DBUS_TYPE_STRING, &(header->kp_id),
			    DBUS_TYPE_INT32, &(header->tr_id),
			    DBUS_TYPE_STRING, &(req_msg->sub_id),
			    DBUS_TYPE_INVALID) )
    {
      g_mutex_lock(param->sib->subscriptions_lock);
      sub = (subscription_state*)g_hash_table_lookup(param->sib->subs, req_msg->sub_id);
      g_mutex_unlock(param->sib->subscriptions_lock);

      if (NULL != sub)
	{
	  // printf("UNSUBSCRIBE: Found sub for sub id %s\n", req_msg->sub_id);

	  /* Set subscription status to stopped */
	  g_mutex_lock(param->sib->subscriptions_lock);
	  // sub->unsub_lock = unsub_lock;
	  sub->unsub_cond = unsub_cond;
	  sub->unsub = FALSE;
	  sub->status = M3_SUB_STOPPED;
	  g_mutex_unlock(param->sib->subscriptions_lock);

	  /* Signal scheduler to execute a round to get
	     all subscriptions to check their status
	     TODO: better way to signal subscriptions
	  */
	  g_mutex_lock(param->sib->new_reqs_lock);
	  param->sib->new_reqs = TRUE;
	  g_cond_signal(param->sib->new_reqs_cond);
	  g_mutex_unlock(param->sib->new_reqs_lock);

	  /* Wait until subscription processing has finished */
	  // g_mutex_lock(unsub_lock);
	  g_mutex_lock(param->sib->subscriptions_lock);
	  while(!sub->unsub)
	    {
	      g_cond_wait(unsub_cond, param->sib->subscriptions_lock);
	    }
	  sub->unsub = FALSE;
	  g_mutex_unlock(param->sib->subscriptions_lock);

	  rsp_msg->status = ss_StatusOK;
	}
      else
	rsp_msg->status = ss_KPErrorRequest;

      whiteboard_util_send_method_return(param->conn,
					 param->msg,
					 DBUS_TYPE_STRING, &(header->space_id),
					 DBUS_TYPE_STRING, &(header->kp_id),
					 DBUS_TYPE_INT32, &(header->tr_id),
					 DBUS_TYPE_INT32, &(rsp_msg->status),
					 DBUS_TYPE_STRING, &(req_msg->sub_id),
					 WHITEBOARD_UTIL_LIST_END);

      printf("UNSUBSCRIBE: Sent unsub cnf for sub id %s\n", req_msg->sub_id);
      g_free(req_msg);
      g_free(rsp_msg);
      g_free(header);
    }
  else
    {
      whiteboard_log_warning("Could not parse UNSUBSCRIBE method call message\n");
    }

  // g_mutex_free(unsub_lock);
  g_cond_free(unsub_cond);
  dbus_message_unref(param->msg);
  g_free(param);
  return NULL;

}


bool triple_callback(DB store, void *data, Node s, Node p, Node o)
{
  m3_triple_int *t, *tmp;
  gchar* key;
  GHashTable** triple_ht = (GHashTable**) data;

  t = g_new0(m3_triple_int, 1);
  t->s = (gint)s;
  t->p = (gint)p;
  t->o = (gint)o;
  key = g_strdup_printf("%d_%d_%d", t->s, t->p, t->o);
  tmp = g_hash_table_lookup(*triple_ht, key);
  //printf("triple_callback: got triple %d %d %d\n", t->s, t->p, t->o);
  if (NULL != tmp)
    {
      /* Free the previous instance of duplicate triple */
      /* The triple values are used after hash table has been destroyed */
      g_free(tmp);
      g_hash_table_insert(*triple_ht, key, t);
    }
  else
    {
      g_hash_table_insert(*triple_ht, key, t);
    }
  return true;
}

gint ssElement_t_to_node(DB store, ssElement_t str_node, ssElementType_t type, ssStatus_t *status)
{
  gint node = 0;
  char* str_tmp = NULL;
  if (0 == g_strcmp0((const char*)str_node, "sib:any") ||
      0 == g_strcmp0((const char*)str_node, "http://www.nokia.com/NRC/M3/sib#any"))
    {
      node = 0;
      return node;
    }

  if (ssElement_TYPE_URI == type)
    {
      str_tmp = piglet_expand_m3(store, (char*)str_node);
      node = piglet_node(store, str_tmp);
      /* printf("Str node %s was mapped to int %d\n", str_tmp, node); */
      free(str_tmp);
    }
  else
    {
      node = piglet_literal(store, (char*)str_node, 0, NULL);
    }

  if (0 == node && strcmp(piglet_error_message, PIGLET_ERR_NODE_NEW))
    {
      *status = ss_OperationFailed;
    }

  return node;
}

ssElement_t node_to_ssElement_t(DB store, gint node, ssStatus_t *status)
{
  gchar *uri = NULL;
  char *tmp_str = NULL;
  char *exp_tmp_str = NULL;
  gint dt = 0;
  char *lang = NULL;

  if (0 == node)
    {
      uri = g_strdup("http://www.nokia.com/NRC/M3/sib#any");
      return (ssElement_t)uri;
    }
  tmp_str = piglet_info(store, node, &dt, lang);
  if (NULL == tmp_str && strcmp(piglet_error_message, PIGLET_ERR_NODE_FIND))
    {
      *status = ss_OperationFailed;
    }
  else
    {
      if (node > 0)
	{
	  exp_tmp_str = piglet_expand_m3(store, tmp_str);
	  uri = g_strdup((gchar*)exp_tmp_str);
	  free(exp_tmp_str);
	}
      else
	{
	  uri = g_strdup((gchar*)tmp_str);
	}
      free(tmp_str);
    }
  return (ssElement_t)uri;
}

ssTriple_t* m3_triple_int_to_ssTriple_t(DB store, m3_triple_int *m3_t, ssStatus_t *status)
{
  ssTriple_t *wb_t;
  wb_t = g_new0(ssTriple_t, 1);
  wb_t->subject = node_to_ssElement_t(store, (gint)m3_t->s, status);
  if (ss_StatusOK != *status)
    {
      goto error;
    }

  wb_t->predicate = node_to_ssElement_t(store, (gint)m3_t->p, status);
  if (ss_StatusOK != *status)
    {
      goto error;
    }

  wb_t->object = node_to_ssElement_t(store, (gint)m3_t->o, status);
  if (ss_StatusOK != *status)
    {
      goto error;
    }
  /* Set correct types for s, p and o */
  wb_t->subjType = ssElement_TYPE_URI; /* Can't be bNode at this stage */
  if (m3_t->o < 0)
    {
      wb_t->objType = ssElement_TYPE_LIT;
    }
  else
    {
      wb_t->objType = ssElement_TYPE_URI;
    }
  return wb_t;

 error:
  return NULL;
}

m3_triple_int* ssTriple_t_to_m3_triple_int(DB store, ssTriple_t *wb_t, ssStatus_t *status)
{
  m3_triple_int *m3_t;
  char *lang = NULL;
  int dt = 0;

  m3_t = g_new0(m3_triple_int, 1);
  m3_t->s = ssElement_t_to_node(store, wb_t->subject, (ssElementType_t)wb_t->subjType, status);
  if (ss_StatusOK != *status)
    {
      goto error;
    }

  m3_t->p = ssElement_t_to_node(store, wb_t->predicate, ssElement_TYPE_URI, status);
  if (ss_StatusOK != *status)
    {
      goto error;
    }
  /* OLD CODE
  if (wb_t->objType == ssElement_TYPE_LIT)
    {
      m3_t->o = piglet_literal(store, (char*)wb_t->object, dt, lang);
      if (!m3_t->o && strcmp(piglet_error_message, PIGLET_ERR_NODE_NEW))
	{
	  *status = ss_OperationFailed;
	  goto error;
	}
    }
  else
    {
      m3_t->o = ssElement_t_to_node(store, (char*)wb_t->object, (ssElementType_t)wb_t->objType, status);
      if (ss_StatusOK != *status)
	{
	  goto error;
	}
    }
  */
  m3_t->o = ssElement_t_to_node(store, wb_t->object, (ssElementType_t)wb_t->objType, status);
  if (ss_StatusOK != *status)
    {
      goto error;
    }
  return m3_t;

 error:
  return NULL;

}


ssStatus_t rdf_writer(scheduler_item* op, sib_data_structure* param)
{
  PigletStatus success;

  switch (op->req->encoding)
    {
    case EncodingM3XML:
      whiteboard_log_debug("Writing RDF/M3 in transaction %d\n", op->header->tr_id);
      GSList* i;
      ssTriple_t* t;
      m3_triple_int* t_int;
      piglet_transaction(param->RDF_store);
      for (i = op->req->insert_graph;
	   i != NULL;
	   i = i->next)
	{
	  t = (ssTriple_t*)i->data;

	  /*
	  whiteboard_log_debug("Got s: %s, p: %s, o: %s",
			       (unsigned char*)t->subject,
			       (unsigned char*)t->predicate,
			       (unsigned char*)t->object);
	  */
	  t_int = ssTriple_t_to_m3_triple_int(param->RDF_store, t, &(op->rsp->status));
	  if (op->rsp->status != ss_StatusOK)
	    {
	      /* Free the created m3_triple_int*/
	      g_free(t_int);
	      goto error;
	    }

	  piglet_add(param->RDF_store, t_int->s, t_int->p, t_int->o, 0, false);
	  piglet_add_post_process(param->RDF_store, t_int->s, t_int->p, t_int->o);
	  /* Free the created m3_triple_int*/
	  g_free(t_int);
	}
      piglet_commit(param->RDF_store);

      op->rsp->status = ss_StatusOK;
      break;

    error:
      piglet_rollback(param->RDF_store);

      op->rsp->status = ss_OperationFailed;
      break;

    case EncodingRDFXML:
      success = piglet_load_m3(param->RDF_store, 0,
			       (unsigned char*)op->req->insert_str,
			       false);
      if (success)
	op->rsp->status = ss_StatusOK;
      else
	op->rsp->status = ss_OperationFailed;
      /* No bnodes to uri mapping from RDF/XML content */
      op->rsp->bnodes_str = g_strdup("<urilist></urilist>");
      break;
    default: /* ERROR CASE */
      op->rsp->status = ss_InvalidParameter;
      op->rsp->bnodes_str = g_strdup("<urilist></urilist>");
      break;
    }
  return op->rsp->status;
}

ssStatus_t rdf_retractor(scheduler_item* op, sib_data_structure* param)
{
  whiteboard_log_debug("Removing in transaction %d\n", op->header->tr_id);

  GSList *i, *rm_list;
  ssTriple_t* t;
  m3_triple_int *t_int, *t_int_iter, *tmp;
  GHashTableIter iter;
  gchar *key, *key_iter;
  GHashTable* rm_triples_hash = g_hash_table_new_full(g_str_hash, g_str_equal,
						      g_free, NULL);
  rm_list = NULL;

  for (i = op->req->remove_graph; i != NULL; i = i->next)
    {
      t = (ssTriple_t*)i->data;
      if (!t->subject || !t->predicate || !t->object)
	{
	  goto error;
	}

      t_int = ssTriple_t_to_m3_triple_int(param->RDF_store, t, &(op->rsp->status));

      if (op->rsp->status != ss_StatusOK)
	{
	  goto error;
	}

      if (t_int->s == 0 || t_int->p == 0 || t_int->o == 0)
	{
	  piglet_query(param->RDF_store, t_int->s, t_int->p, t_int->o, 0,
		       &rm_triples_hash, triple_callback);
 	}
      else
	{
	  key = g_strdup_printf("%d_%d_%d", t_int->s, t_int->p, t_int->o);
	  tmp = g_hash_table_lookup(rm_triples_hash, key);
	  if (NULL != tmp)
	    {
	      /* Free the previous instance of duplicate triple */
	      g_free(tmp);
	      g_hash_table_insert(rm_triples_hash, key, t_int);
	    }
	  else
	    {
	      g_hash_table_insert(rm_triples_hash, key, t_int);
	    }
	  //printf("RDFRETRACTOR: adding to rm_list triple %d %d %d\n", t_int->s, t_int->p, t_int->o);
	  //rm_list = g_slist_prepend(rm_list, t_int);
	}

    }

  g_hash_table_iter_init(&iter, rm_triples_hash);
  while(g_hash_table_iter_next(&iter, (gpointer*)&key_iter, (gpointer*)&t_int_iter))
    {
      printf("RDFRETRACTOR: adding to rm_list triple %d %d %d from HT\n", t_int_iter->s, t_int_iter->p, t_int_iter->o);
      rm_list = g_slist_prepend(rm_list, t_int_iter);
    }

  for (i = rm_list; i != NULL; i = i->next)
    {
      t_int = (m3_triple_int*)i->data;
      piglet_del(param->RDF_store, t_int->s, t_int->p, t_int->o, 0, false);
      printf("RDFRETRACTOR: Deleted triple %d %d %d\n", t_int->s, t_int->p, t_int->o);
      printf("RDFRETRACTOR: Deleted triple\n %s\n %s\n %s\n", node_to_ssElement_t(param->RDF_store, t_int->s, &(op->rsp->status)),
	     node_to_ssElement_t(param->RDF_store, t_int->p, &(op->rsp->status)),
	     node_to_ssElement_t(param->RDF_store, t_int->o, &(op->rsp->status)));
    }

  //printf("XXX RETRACTOR: Now freeing triple int list in transaction %d\n", op->header->tr_id);
  m3_free_triple_int_list(&rm_list, NULL);
  g_hash_table_destroy(rm_triples_hash);
  op->rsp->status = ss_StatusOK;
  return op->rsp->status;
 error:
  m3_free_triple_int_list(&rm_list, NULL);
  g_hash_table_destroy(rm_triples_hash);
  op->rsp->status = ss_OperationFailed;
  return op->rsp->status;
}

ssStatus_t rdf_reader(scheduler_item* op, sib_data_structure* p)
{
  whiteboard_log_debug("Querying in transaction %d\n", op->header->tr_id);
  char* str_tmp_exp;

  switch (op->req->type)
    {
    case QueryTypeTemplate:
      {
	ssTriple_t* tq;
	m3_triple_int *tq_int, *t;
	GSList* query_list = op->req->template_query;
	GHashTableIter iter;
	gchar* key;
	GHashTable* results = g_hash_table_new_full(g_str_hash, g_str_equal,
						    g_free, NULL);

	whiteboard_log_debug("Doing template query");
	/* Changed if anything goes wrong */
	op->rsp->status = ss_StatusOK;

	/* Iterate over query here */
	while (query_list != NULL) {
	  tq = (ssTriple_t*)query_list->data;
	  if (!tq->subject || !tq->predicate || !tq->object)
	    {
	      op->rsp->status = ss_OperationFailed;
	      op->rsp->results = NULL;
	      break;
	    }
	  printf("RDF reader: now querying for transaction %d\n", op->header->tr_id);
	  printf("RDF reader: triple is %s\n%s\n%s\n", tq->subject, tq->predicate, tq->object);
	  tq_int = ssTriple_t_to_m3_triple_int(p->RDF_store, tq, &(op->rsp->status));
	  if (op->rsp->status != ss_StatusOK)
	    {
	      op->rsp->status = ss_OperationFailed;
	      op->rsp->results = NULL;
	      break;
	    }
	  piglet_query(p->RDF_store, tq_int->s, tq_int->p, tq_int->o, 0, &results, triple_callback);

	  g_free(tq_int->lang);
	  g_free(tq_int);
	  query_list = query_list->next;
	}
	if (op->rsp->status == ss_StatusOK)
	  {
	    g_hash_table_iter_init(&iter, results);
  	    while(g_hash_table_iter_next(&iter, (gpointer*)&key, (gpointer*)&t))
	      {
		op->rsp->results = g_slist_prepend(op->rsp->results, t);
	      }
	    g_hash_table_destroy(results);
	  }
	else
	  {
	    /* If something went wrong, free results */
	    g_hash_table_iter_init(&iter, results);
  	    while(g_hash_table_iter_next(&iter, (gpointer*)&key, (gpointer*)&t))
	      {
		g_free(t);
	      }
	    g_hash_table_destroy(results);
	    op->rsp->results = NULL;
	  }
	break;
      }
#if WITH_WQL==1
    case QueryTypeWQLValues:
      {
	whiteboard_log_debug("Doing WQL values query");
	unsigned char* node_str;
	gchar* path;
	gint node;
	ssElementType_t node_type;

	node_str = op->req->wql_query->wqlType.values.startNode->string;
	path = op->req->wql_query->wqlType.values.pathExpr;
	node_type = op->req->wql_query->wqlType.values.startNode->nodeType;

	whiteboard_log_debug("StartNode: %s", node_str);
	whiteboard_log_debug("path: %s", path);
	whiteboard_log_debug("type: %d", node_type);

	if (!node_str || !path)
	  {
	    op->rsp->status = ss_OperationFailed;
	    op->rsp->results = NULL;
	    break;
	  }
	piglet_transaction(p->RDF_store);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)node_str);
	if (node_type == ssElement_TYPE_URI)
	  {
	    node = piglet_node(p->RDF_store, str_tmp_exp);
	  }
	else
	  {
	    node = piglet_literal(p->RDF_store, str_tmp_exp, 0, NULL);
	  }
	free(str_tmp_exp);

	op->rsp->results = p_call_values(p->p_w, node, path);

	piglet_commit(p->RDF_store);

	op->rsp->status = ss_StatusOK;
	break;
      }
    case QueryTypeWQLNodeTypes:
      {
	whiteboard_log_debug("Doing WQL nodetypesquery");
	unsigned char* node_str;
	gint node;

	node_str = op->req->wql_query->wqlType.nodeTypes.node->string;
	if (!node_str)
	  {
	    op->rsp->status = ss_OperationFailed;
	    op->rsp->results = NULL;
	    break;
	  }
	piglet_transaction(p->RDF_store);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)node_str);
	node = piglet_node(p->RDF_store, str_tmp_exp);
	free(str_tmp_exp);
	op->rsp->results = p_call_nodetypes(p->p_w, node);

	piglet_commit(p->RDF_store);

	op->rsp->status = ss_StatusOK;
	break;
      }

    case QueryTypeWQLRelated:
      {
	whiteboard_log_debug("Doing WQL related query");
	unsigned char *source_str, *sink_str;
	gchar *path;
	gint source, sink;
	ssElementType_t source_type, sink_type;


	path = op->req->wql_query->wqlType.related.pathExpr;
	source_str = op->req->wql_query->wqlType.related.startNode->string;
	sink_str = op->req->wql_query->wqlType.related.endNode->string;
	source_type = op->req->wql_query->wqlType.related.startNode->nodeType;
	sink_type = op->req->wql_query->wqlType.related.endNode->nodeType;

	if (!source_str || !sink_str || !path)
	  {
	    op->rsp->status = ss_OperationFailed;
	    op->rsp->bool_results = false;
	    break;
	  }
	piglet_transaction(p->RDF_store);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)source_str);

	if (source_type == ssElement_TYPE_URI)
	  {
	    source = piglet_node(p->RDF_store, str_tmp_exp);
	  }
	else
	  {
	    source = piglet_literal(p->RDF_store, str_tmp_exp, 0, NULL);
	  }
	free(str_tmp_exp);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)sink_str);
	if (sink_type == ssElement_TYPE_URI)
	  {
	    sink = piglet_node(p->RDF_store, str_tmp_exp);
	  }
	else
	  {
	    sink = piglet_literal(p->RDF_store, str_tmp_exp, 0, NULL);
	  }
	free(str_tmp_exp);

	op->rsp->bool_results = p_call_related(p->p_w, source, path, sink);

	piglet_commit(p->RDF_store);

	op->rsp->status = ss_StatusOK;
	break;
      }
    case QueryTypeWQLIsType:
      {
	whiteboard_log_debug("Doing WQL istype query");
	unsigned char *node_str, *type_str;
	gint node, type;

	node_str = op->req->wql_query->wqlType.isType.node->string;
	type_str = op->req->wql_query->wqlType.isType.typeNode->string;
	if (!node_str || !type_str)
	  {
	    op->rsp->status = ss_OperationFailed;
	    op->rsp->bool_results = false;
	    break;
	  }
	piglet_transaction(p->RDF_store);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)node_str);
	node = piglet_node(p->RDF_store, str_tmp_exp);
	free(str_tmp_exp);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)type_str);
	type = piglet_node(p->RDF_store, str_tmp_exp);
	free(str_tmp_exp);

	op->rsp->bool_results = p_call_istype(p->p_w, node, type);

	piglet_commit(p->RDF_store);

	op->rsp->status = ss_StatusOK;
	break;
      }
    case QueryTypeWQLIsSubType:
      {
	whiteboard_log_debug("Doing WQL issubtype query");
	unsigned char *sub_str, *super_str;
	gint sub, super;

	sub_str = op->req->wql_query->wqlType.isSubType.subTypeNode->string;
	super_str = op->req->wql_query->wqlType.isSubType.superTypeNode->string;
	if (!sub_str || !super_str)
	  {
	    op->rsp->status = ss_OperationFailed;
	    op->rsp->bool_results = false;
	    break;
	  }
	piglet_transaction(p->RDF_store);


	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)sub_str);
	sub = piglet_node(p->RDF_store, str_tmp_exp);
	free(str_tmp_exp);

	str_tmp_exp = piglet_expand_m3(p->RDF_store, (char*)super_str);
	super = piglet_node(p->RDF_store, str_tmp_exp);
	free(str_tmp_exp);

	op->rsp->bool_results = p_call_issubtype(p->p_w, sub, super);

	piglet_commit(p->RDF_store);

	op->rsp->status = ss_StatusOK;
	break;
      }
#endif /* WITH_WQL */
    case QueryTypeSPARQLSelect:
      {
	whiteboard_log_debug("SPARQL query not yet implemented");
	op->rsp->status = ss_SIBFailNotImpl;
	break;
      }
    default:
      op->rsp->status = ss_InvalidParameter;
      break; /* ERROR */
    }

  if (op->header->tr_type == M3_SUBSCRIBE) /* SUB_DEBUG */
    printf("Processed subscription %s\n", op->rsp->sub_id); /* SUB_DEBUG */
  else /* SUB_DEBUG */
    printf("Processed query %d\n", op->header->tr_id); /* SUB_DEBUG */

  if (op->header->tr_type == M3_SUBSCRIBE && op->rsp->status == ss_StatusOK)
    {
      subscription_state* s;
      g_mutex_lock(p->subscriptions_lock);
      s = g_hash_table_lookup(p->subs, op->rsp->sub_id);
      if (NULL != s && s->status != M3_SUB_STOPPED)
	{
	  s->status = M3_SUB_ONGOING;
	  printf("Set subscription %s to ongoing\n", s->sub_id); /* SUB_DEBUG */
	}
      g_mutex_unlock(p->subscriptions_lock);
    }
  return op->rsp->status;
}

void do_insert(gpointer op_param, gpointer p_param)
{
  scheduler_item* op = (scheduler_item*) op_param;
  sib_data_structure* p = (sib_data_structure*) p_param;
  switch (op->header->tr_type)
    {
    case M3_INSERT:
      g_mutex_lock(op->op_lock);
      g_mutex_lock(p->store_lock);
      whiteboard_log_debug("Beginning to insert for transaction %d\n", op->header->tr_id);
      rdf_writer(op, p);
      whiteboard_log_debug("Done inserting for transaction %d\n", op->header->tr_id);
      g_mutex_unlock(p->store_lock);
      op->op_complete = TRUE;
      // printf("Now signaling transaction %d for finished operation\n", op->header->tr_id);
      g_cond_signal(op->op_cond);
      g_mutex_unlock(op->op_lock);
      break;
    case M3_REMOVE:
      g_mutex_lock(op->op_lock);
      g_mutex_lock(p->store_lock);
      whiteboard_log_debug("Beginning to remove for transaction %d\n", op->header->tr_id);
      rdf_retractor(op, p);
      whiteboard_log_debug("Done removing for transaction %d\n", op->header->tr_id);
      g_mutex_unlock(p->store_lock);
      op->op_complete = TRUE;
      g_cond_signal(op->op_cond);
      g_mutex_unlock(op->op_lock);
      break;
    case M3_UPDATE:
      g_mutex_lock(op->op_lock);
      g_mutex_lock(p->store_lock);
      whiteboard_log_debug("Beginning to update for transaction %d\n", op->header->tr_id);
      rdf_retractor(op, p);
      rdf_writer(op, p);
      whiteboard_log_debug("Done updating for transaction %d\n", op->header->tr_id);
      g_mutex_unlock(p->store_lock);
      op->op_complete = TRUE;
      g_cond_signal(op->op_cond);
      g_mutex_unlock(op->op_lock);
      break;

      /*AD-ARCES*/
      case M3_PROTECTION_FAULT:
            /* SIB PROTECTION FAULT CASE */
          	printf("----> SIB PROTECTION FAULT CASE\n");
            g_mutex_lock(op->op_lock);

            /*AD-ARCES*/
            op->rsp->status = ss_SIBProtectionFault;// ss_SIBFailAccessDenied;// ss_InvalidParameter;// ss_OperationFailed;//

            /*AD-ARCES*/
            op->op_complete = TRUE; //FALSE; //it is just for lock purpose


            g_cond_signal(op->op_cond);
            g_mutex_unlock(op->op_lock);
            break;


    default:
      /* ERROR CASE */
      g_mutex_lock(op->op_lock);
      op->rsp->status = ss_InvalidParameter;
      op->op_complete = TRUE;
      g_cond_signal(op->op_cond);
      g_mutex_unlock(op->op_lock);
      break;
    }
}

void do_query(gpointer op_param, gpointer p_param)
{
  scheduler_item* op = (scheduler_item*) op_param;
  sib_data_structure* p = (sib_data_structure*) p_param;
  switch (op->header->tr_type)
    {
    /* Query and subscribe handled similarly at this level (for now) */
    case M3_SUBSCRIBE:
      /* Fallthrough */
    case M3_QUERY:
      g_mutex_lock(op->op_lock);
      g_mutex_lock(p->store_lock);
      whiteboard_log_debug("Beginning to query for transaction %d\n", op->header->tr_id);
      rdf_reader(op,p);
      whiteboard_log_debug("Done querying for transaction %d\n", op->header->tr_id);
      g_mutex_unlock(p->store_lock);
      op->op_complete = TRUE;
      g_cond_signal(op->op_cond);
      g_mutex_unlock(op->op_lock);
      break;
    default:
      /* ERROR CASE */
      g_mutex_lock(op->op_lock);
      op->rsp->status = ss_InvalidParameter;
      op->op_complete = TRUE;
      g_cond_signal(op->op_cond);
      g_mutex_unlock(op->op_lock);
      break;
    }
}

void set_sub_to_pending(gpointer sub_id, gpointer sub_data, gpointer unused)
{
  subscription_state* s = (subscription_state*)sub_data;
  if (s->status != M3_SUB_STOPPED)
    s->status = M3_SUB_PENDING;
  printf("Set subscription %s to pending\n", s->sub_id); /* SUB_DEBUG */

}

void set_sub_to_ongoing(gpointer sub_id, gpointer sub_data, gpointer unused)
{
  subscription_state* s = (subscription_state*)sub_data;
  if (s->status != M3_SUB_STOPPED)
    s->status = M3_SUB_ONGOING;
  printf("Set subscription %s to ongoing\n", s->sub_id); /* SUB_DEBUG */
}

/*
 * Scheduler (or serializer) to read operation requests from insert and query
 * queue one at a time and call corresponding function to handle the
 * communication with wilbur.
 *
 * When new inserts have been made, read contents of insert and query queues
 * to the corresponding lists and process these 1. insert 2. query
 * Check that subscriptions do not miss any inserts
 */

gpointer scheduler(gpointer data)
{
  sib_data_structure* p = (sib_data_structure*)data;

  GAsyncQueue* i_queue = p->insert_queue;
  GAsyncQueue* q_queue = p->query_queue;
  /* GAsyncQueue* s_queue = p->subscribe_queue; */

  GCond* new_reqs_cond = p->new_reqs_cond;
  GMutex* new_reqs_lock = p->new_reqs_lock;
  /* gboolean new_reqs = p->new_reqs; */
  GHashTable* subs = p->subs;
  GMutex* subscriptions_lock = p->subscriptions_lock;

#if WITH_WQL==1
  p_wilbur_functions* p_w;

  PyObject* p_class;
#endif /* WITH_WQL */

  gboolean updated = false;

  GSList* i_list = NULL;
  GSList* q_list = NULL;
  /* GSList* s_list = NULL; */

  scheduler_item* op;

#if WITH_WQL==1
  g_mutex_lock(p->scheduler_init_lock);
  /* Initialize python interpreter */

  Py_Initialize();
  PyEval_InitThreads();
  p_w = g_new0(p_wilbur_functions, 1);

  /* Load wilbur python module */

  p_w->p_module = PyImport_ImportModule(PYTHON_WILBUR_MODULE);
  if (NULL == p_w->p_module) {
    PyErr_Print();
    exit(-1);
  }
  p_class = PyObject_GetAttrString(p_w->p_module, "DB");
  if (NULL == p_class) {
    PyErr_Print();
    exit(-1);
  }
  p_w->p_instance = PyObject_CallFunction(p_class, "(s)", p->ss_name);
  if (NULL == p_w->p_instance) {
    PyErr_Print();
    exit(-1);
  }

  p->p_w = p_w;
  p->scheduler_init = TRUE;
  g_cond_signal(p->scheduler_init_cond);
  g_mutex_unlock(p->scheduler_init_lock);
#endif /* WITH_WQL */

  while (TRUE) {
    /* Wait until there is actually something in the queues */
    g_mutex_lock(new_reqs_lock);
    while (!(p->new_reqs))
      {
	g_cond_wait(new_reqs_cond, new_reqs_lock);
      }
    p->new_reqs = FALSE;
    g_mutex_unlock(new_reqs_lock);

    /* Lock insert and query queues
     * Insert contents into lists for processing
     */
    g_async_queue_lock(i_queue);
    while (NULL !=
	   (op = (scheduler_item*)g_async_queue_try_pop_unlocked(i_queue)))
      {

        /*AD-ARCES*/
    	printf("#######################################\n");
    	printf("PROTECTION CONTROL \n");
    	printf("#######################################\n");

    	//LCTable_Test();
    	ProtectionCompatibilityFilter(op);

    	printf("#######################################\n\n");


	i_list = g_slist_prepend(i_list, op);
	whiteboard_log_debug("Added item to insert list");
      }
    g_async_queue_unlock(i_queue);

    if (i_list != NULL)
      updated = true;

    g_async_queue_lock(q_queue);
    while (NULL !=
	   (op = (scheduler_item*)g_async_queue_try_pop_unlocked(q_queue)))
      {
	q_list = g_slist_prepend(q_list, op);
	whiteboard_log_debug("Added item to query list");
      }
    g_async_queue_unlock(q_queue);

    /*
    if (updated)
      {
	g_async_queue_lock(s_queue);
	while (NULL !=
	       (op = (scheduler_item*)g_async_queue_try_pop_unlocked(s_queue)))
	  {
	    s_list = g_slist_prepend(s_list, op);
	    whiteboard_log_debug("Added item to subscribe list");
	  }
	g_async_queue_unlock(s_queue);
      }
    */
    /*
     * Process both inserts and queries
     */
    g_slist_foreach(i_list, do_insert, p);
    g_slist_free(i_list);
    i_list = NULL;

    if (updated)
      {
	g_mutex_lock(subscriptions_lock);
	g_hash_table_foreach(subs, set_sub_to_pending, NULL);
	g_mutex_unlock(subscriptions_lock);
	printf("RDF store updated, set all subscriptions to pending\n"); /* SUB_DEBUG */
	updated = false;
      }

    /* Process plugin reasoners here */

    g_slist_foreach(q_list, do_query, p);
    g_slist_free(q_list);
    q_list = NULL;

    /*
    g_slist_foreach(s_list, do_query, p);
    g_slist_free(s_list);
    q_list = NULL;
    */
  }
}

/*
 * Initialization of SIB internal data structures
 * Contains
 * - mutexes for KP membership, subscription state and
 *   RDF store access
 * - Hash tables for joined KPs and ongoing subscriptions
 * - Queues for insertions and deletions
 *
 */

sib_data_structure* sib_initialize(gchar* name)
{

  sib_data_structure* sd;

  /* Allocate sib data structures */

  sd = g_new0(sib_data_structure, 1);
  if (NULL == sd) exit(-1);

  /* Initialize smart space name */
  sd->ss_name = name;

  /* DAN-ARCES-2011.03.02 */
  printf("######################################################\n");
  printf("# Initialize smart space name with:%s\n",sd->ss_name);


#if WITH_WQL==1
  sd->scheduler_init_lock = g_mutex_new();
  sd->scheduler_init_cond = g_cond_new();
  g_mutex_lock(sd->scheduler_init_lock);
#endif /* WITH_WQL */

  sd->insert_queue = g_async_queue_new();
  if (NULL == sd->insert_queue) exit(-1);

  sd->query_queue = g_async_queue_new();
  if (NULL == sd->query_queue) exit(-1);

  /*
  sd->subscribe_queue = g_async_queue_new();
  if (NULL == sd->subscribe_queue) exit(-1);
  */

  sd->joined = g_hash_table_new(g_str_hash, g_str_equal);
  if (NULL == sd->joined) exit(-1);

  sd->subs = g_hash_table_new(g_str_hash, g_str_equal);
  if (NULL == sd->subs) exit(-1);

  sd->members_lock = g_mutex_new();
  if (NULL == sd->members_lock) exit(-1);

  sd->subscriptions_lock = g_mutex_new();
  if (NULL == sd->subscriptions_lock) exit(-1);

  sd->store_lock = g_mutex_new();
  if (NULL == sd->store_lock) exit(-1);

  sd->new_reqs_lock = g_mutex_new();
  if (NULL == sd->new_reqs_lock) exit(-1);

  sd->new_reqs_cond = g_cond_new();
  if (NULL == sd->new_reqs_cond) exit(-1);

  sd->new_reqs = FALSE;


  /* Start scheduler */
  g_thread_create(scheduler, sd, FALSE, NULL);

#if WITH_WQL==1

  sd->scheduler_init = FALSE;
  while(!sd->scheduler_init)
    g_cond_wait(sd->scheduler_init_cond, sd->scheduler_init_lock);
  g_mutex_unlock(sd->scheduler_init_lock);

  sd->RDF_store = p_call_get_db(sd->p_w);
#else /* WITH_WQL */

  sd->RDF_store = piglet_open(sd->ss_name);
  if (NULL == sd->RDF_store) exit(-1);

#endif /* WITH_WQL */

#if WITH_WQL==1
  g_mutex_free(sd->scheduler_init_lock);
  g_cond_free(sd->scheduler_init_cond);
#endif /* WITH_WQL */
  return sd;
}
