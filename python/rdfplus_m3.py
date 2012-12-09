


#   Copyright (c) 2009, Nokia Corporation
#   All rights reserved.

#   Redistribution and use in source and binary forms, with or without
#   modification, are permitted provided that the following conditions
#   are met:
  
#     * Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.  
#     * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.  
#     * Neither the name of Nokia nor the names of its contributors 
#     may be used to endorse or promote products derived from this 
#     software without specific prior written permission.

#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
#   FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
#   COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
#   INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
#   HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
#   CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
#   OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
#   EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#
#   rdfplus.py
#
#   Author: Ora Lassila mailto:ora.lassila@nokia.com
#   Copyright (c) 2001-2008 Nokia. All Rights Reserved.
#

import wilbur_m3

class DB(wilbur_m3.DB):
    def postProcess(self, source):
        self.db.transaction()
        try:
            for (s, p, o) in self.query(0, 0, 0, source):
                self.addPostProcess(s, p, o, 0)
        except:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def bootstrap(self):
        super(DB, self).bootstrap()
        self.clearReasonerCache()
        self.saClusters = {}
        self.saQuery = self.qe.fsa(['rep*', ['or', self.sa, ['inv', self.sa]]])
        self.subprops = [self.subprop]

    def clearReasonerCache(self):
        self.qe.fsaCache = {}
        self.subpropQuery = None
        self.rewrittenPaths = {}

    def getSubpropQuery(self):
        q = self.subpropQuery
        if q == None:
            self.rewrittenPaths = {}
            q = self.qe.fsa(['rep*', ['inv', ['or'] + self.subprops]])
            self.subpropQuery = q
        return q

    def load(self, source, verbose=True, seed=False):
        if super(DB, self).load(source, verbose, seed):
            if not seed:
                self.postProcess(source if isinstance(source, int) else self.node(source))
            return True
        else:
            return False

    def add(self, s, p, o, source=0, temporary=False):
        if super(DB, self).add(s, p, o, source, temporary):
            self.addPostProcess(s, p, o, source)
            return True
        else:
            return False

    def addPostProcess(self, s, p, o, source):
        self.add(p, self.type, self['rdf:Property'], self.reasoner, True)
        if p == self.type:
            self.add(o, self.type, self['rdfs:Class'], self.reasoner, True)
            self.add(o, self.subclass, self.resource, self.reasoner, True)
        elif p == self.subclass:
            self.add(o, self.subclass, self.resource, self.reasoner, True)
        elif p == self.sa:
            self.updateSameas(s)
        elif p in self.subprops:
            self.clearReasonerCache()
            if o in self.subprops:
                self.subprops = self.values(self.subprop, self.getSubpropQuery(), False)
                self.subpropQuery = None
        elif o < 0:
            (str, dt, lang) = self.info(o)
            if dt:
                self.add(o, self.type, self['rdf:XMLLiteral'], self.reasoner, True)
            elif self.related(p, self.subprop, self['dc:date']):
                (date, t) = self.literalParser.iso8601.parse(str)
                if date:
                    newstr = date.isoformat()
                    newdt = self['xsd:datetime' if t else 'xsd:date']
                    if (str == newstr):
                        succ = self.augmentLiteral(o, newdt)
                    else:
                        oo = self.literal(newstr, newdt, "")
                        super(DB, self).add(s, p, oo, source)
                        self.delete(s, p, o, source)
                        o = oo
                    self.add(o, self.type, self['rdf:XMLLiteral'], self.reasoner, True)

    def delete(self, s, p, o, source=0, temporary=False):
        if super(DB, self).delete(s, p, o, source, temporary):
            if p == self.sa:
                self.updateSameas(s)
                self.updateSameas(o)
            return True
        else:
            return False

    def _str_to_node_wql(self, str_list):
        WQL_WORDS = ['seq', 'seq+', 'or', 'rep*', 'rep+', 'inv',
                     'value', 'norewrite', 'filter', 'any',
                     'members', 'p-of-s', 'p-of-o', 'self']
        path_expr = []
        for i_str in str_list:
            if type(i_str) == list:
                path_expr.append(self._str_to_node_wql(i_str))
            elif i_str in WQL_WORDS:
                path_expr.append(i_str)
            else:
                # i_str = expand(space, i_str)
                i_str = self.db.expand_m3(i_str)
                i = self.db.node(i_str)
                path_expr.append(i)
        return path_expr

    def values_m3(self, node, path, reasoner=True):
        str_path = eval(path)
        path_expr = self._str_to_node_wql(str_path)
        #print "WQL VALUES QUERY M3:            node:", node
        #print "WQL VALUES QUERY M3:            path:", path
        #print "WQL VALUES QUERY M3: path expression:", path_expr
        retval = self.values(node, path_expr, reasoner)
        #print "WQL VALUES QUERY M3:          result:", retval
        return retval.__iter__()

    def values(self, node, path, reasoner=True):
        #print "WQL VALUES QUERY:               node:", node
        #print "WQL VALUES QUERY:               path:", path
        if reasoner:
            sameas = self.saClusters.get(node)
            if path == self.sa:
                return (sameas or [])
            elif sameas:
                results = set()
                for n in sameas:
                    results |= self.valuesInner(n, path, reasoner)
                return list(results)
            else:
                return self.valuesInner(node, path, reasoner)
        else:
            return super(DB, self).values(node, path, False)

    def valuesInner(self, node, path, reasoner):
        if isinstance(path, wilbur_m3.InverseNode) and node == self.resource:
            if path.node in [self.type, self.subclass]:
                return [0]
            else:
                return super(DB, self).values(node, path, False)
        elif reasoner:
            return self.values(node, self.rewritePath(path), False)
        else:
            return super(DB, self).values(node, path, False)

    def related_m3(self, source, path, sink, reasoner=True):
        str_path = eval(path)
        path_expr = self._str_to_node_wql(str_path)
        return self.related(source, path_expr, sink, reasoner)
        

    def related(self, source, path, sink, reasoner=True):
        if not reasoner:
            return super(DB, self).related(source, path, sink, False)
        elif isinstance(path, wilbur_m3.InverseNode) and node == self.resource:
            if path.node in [self.type, self.subclass]:
                return True
            else:
                return super(DB, self).related(source, path, sink, False)
        else:
            return self.related(source, self.rewritePath(path), sink, False)

    def updateSameas(self, node):
        sameas = self.values(node, self.saQuery, False)
        if len(sameas) > 1:
            for i in sameas:
                self.saClusters[i] = sameas
        else:
            del self.saClusters[node]

    def newMemberProp(self, i):
        prop = super(DB, self).newMemberProp(i)
        self.add(prop, self.type, self['rdfs:ContainerMembershipProperty'], 0, True)
        self.add(prop, self.subprop, self['rdfs:member'], 0, True)
        return prop

    def rewritePath(self, path):
        r = repr(path)
        p = self.rewrittenPaths.get(r)
        if not p:
            p = self.rewritePathInner(path, self.getPathRewriters())
            self.rewrittenPaths[r] = p
        return p

    def rewritePathInner(self, path, rewriters):
        if isinstance(path, list):
            return [path[0]] + [self.rewritePathInner(p, rewriters) for p in path[1:]]
        elif len(rewriters) == 0:
            return path
        else:
            return self.rewritePathInner(rewriters[0](path), rewriters[1:])

    def getPathRewriters(self):
        return [self.rewritePathForTypes, self.rewritePathForSubprops]

    def rewritePathForTypes(self, path):
        if path == self.type:
            sc = ['rep*', self.subclass]
            return ['or', ['seq', self.type, sc],
                          ['seq', 'p-of-o', self['rdfs:range'],  sc],
                          ['seq', 'p-of-s', self['rdfs:domain'], sc],
                          ['value', self.resource]]
        elif path == self.subclass:
            return ['or', ['rep*', path], ['value', self.resource]]
        elif path == self.subprop:
            return ['rep*', path]
        else:
            return path

    def rewritePathForSubprops(self, path):
        props = self.values(path, self.getSubpropQuery(), False)
        return (['or'] + props if len(props) > 1 else props[0])

    def isSubtype(self, type, super): return self.related(type, self.subclass, super)

    def isType(self, node, type): return self.related(node, self.type, type)

    def sortTypes(self, types):
        queue = [(t, [o for (s, p, o) in self.query(t, self.subclass, 0) if o != t])
                 for t in types]
        print "sortTypes:", queue
        sorted = []
        while (queue):
            for (item, successors) in queue:
                if not successors:
                    sorted.append(item)
                    queue.remove((item, successors))
                    for (i, s) in queue:
                        if item in s:
                            s.remove(item)
                    break
        return sorted[::-1]

    def nodeTypes_m3(self, node): return self.nodeTypes(node).__iter__()
    def nodeTypes(self, node): 
        rval = self.sortTypes(self.values(node, self.type, reasoner = False))
        print "Nodetypes: result", rval
        return rval
