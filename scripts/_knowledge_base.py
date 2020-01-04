import re
import copy
import sys

from _connection import Connection
from _utils import *

class KnowledgeBase:
  current_abox = None
  connection = None

  def __init__(self, configuration = None):
      if not KnowledgeBase.connection:
          KnowledgeBase.connection = Connection('localhost', 8088, 1)
  
      self.abox = None
      self.memory = {}

      self.configuration = configuration
      
  def connect(self, debugging=False):
      
      KnowledgeBase.connection.connect()

      if not debugging:
        KnowledgeBase.connection.writeline('(logging-on)')
        KnowledgeBase.connection.flush()

      KnowledgeBase.connection.writeline('(|OWLAPI-enableSimplifiedProtocol| :|global|)')
      KnowledgeBase.connection.flush()

      if self.configuration:
        self.load(self.configuration)

  def prepare(self):
      KnowledgeBase.connection.writeline('(prepare-abox)')
      KnowledgeBase.connection.flush()

  def close(self):
      KnowledgeBase.connection.close()

  def switch(self):
      KnowledgeBase.connection.writeline('(set-current-abox {0})'.format(self.abox))
      KnowledgeBase.current_abox = KnowledgeBase.connection.readline()

  def load(self, filename):
      KnowledgeBase.connection.writeline('(full-reset)')
      KnowledgeBase.connection.flush()

      KnowledgeBase.connection.writeline(
          '(owl-read-file "{0}")'.format(filename.replace('\\', '/'))
      )
      self.abox = KnowledgeBase.connection.readline()
      KnowledgeBase.current_abox = self.abox

      self.prepare()

  def save(self, filename):
      if KnowledgeBase.current_abox != self.abox:
          self.switch()

      KnowledgeBase.connection.writeline(
          '(save-kb "{0}" :syntax :OWL)'.format(filename.replace('\\', '/'))
      )
      return self.connection.readline()

  def evaluate(self, data):
      if 'arg0' in data:
          raise Exception('arg0 detected')
      if KnowledgeBase.current_abox != self.abox:
          self.switch()

      KnowledgeBase.connection.writeline(data)
      return KnowledgeBase.connection.readline()

  def process(self, result):
      if ':error' in result or 'NIL' in result:
          return []
      return re.findall('#([^|)]+)', result)

  def ask(self, statement):
      if statement in self.memory:
          return self.memory[statement]

      if is_negative(statement):
          return not self.ask(negate(statement))

      retrieve_complement = False

      if is_complement(statement):
          retrieve_complement = True

      terms = logical_split(statement)

      if '?' in terms:
          if len(terms) == 2:
              if terms[0] == '?':
                  return self.cache(statement, self.process(self.evaluate('(individual-direct-types #!:{0})'.format(terms[1]))))

              if retrieve_complement:
                  return self.cache(statement, self.process(self.evaluate('(retrieve (?x) (neg (project-to (?x) (?x #!:{0}))))'.format(terms[0][1:]))))

              return self.cache(statement, self.process(self.evaluate('(concept-instances #!:{0})'.format(terms[0]))))

          if terms[0] == '?':
              return self.cache(statement, self.process(self.evaluate('(individual-filled-roles #!:{0} #!:{1})'.format(terms[2], terms[1]))))

          for idx in range(len(terms)):
              terms[idx] = '?x' if terms[idx] == '?' else '#!:' + (terms[idx][1:] if terms[idx].startswith('!') else terms[idx])

          if retrieve_complement:
              return self.cache(statement, self.process(self.evaluate('(retrieve (?x) (neg (project-to (?x) ({0} {1} {2}))))'.format(terms[2], terms[1], terms[0]))))

          return self.cache(statement, self.process(self.evaluate('(retrieve (?x) ({0} {1} {2}))'.format(terms[2], terms[1], terms[0]))))

      else:
          if len(terms) == 2:
              return self.cache(statement, 'T' in self.evaluate('(individual-instance? #!:{0} #!:{1})'.format(terms[1], terms[0])))

          if terms[0] == 'is_a':
              return self.cache(statement, 'T' in self.evaluate('(concept-subsumes? #!:{0} #!:{1})'.format(terms[1], terms[2])))

          return self.cache(statement, 'T' in self.evaluate('(individuals-related? #!:{0} #!:{1} #!:{2})'.format(terms[2], terms[1], terms[0])))

  def tell(self, statement):
      self.invalidate()

      if is_negative(statement):
          terms = logical_split(negate(statement))
          if len(terms) == 2:
              self.evaluate('(forget-concept-assertion {0} #!:{1} #!:{2})'.format(self.abox, terms[1], terms[0]))
          else:
              self.evaluate('(forget-role-assertion {0} #!:{1} #!:{2} #!:{3})'.format(self.abox, terms[2], terms[1], terms[0]))
      else:
          terms = logical_split(statement)
          if len(terms) == 2:
              self.evaluate('(add-concept-assertion {0} #!:{1} #!:{2})'.format(self.abox, terms[1], terms[0]))
          else:
              if self.evaluate('(feature? #!:{0})'.format(terms[0])) == 'T':
                  previous = self.ask('({0} ? {1})'.format(terms[0], terms[2]))

                  if previous:
                      self.evaluate('(forget-role-assertion {0} #!:{1} #!:{2} #!:{3})'.format(self.abox, terms[2], previous[0], terms[0]))

              self.evaluate('(add-role-assertion {0} #!:{1} #!:{2} #!:{3})'.format(self.abox, terms[2], terms[1], terms[0]))

  def inverse(self, role_name):
      inverse_name = self.process(self.evaluate('(role-inverse #!:{0})'.format(role_name)))
      if not len(inverse_name):
          return None
      return inverse_name[0]

  def clone(self):
      cloned = KnowledgeBase()
      cloned.abox = self.evaluate('(clone-abox {0})'.format(self.abox))
      if cloned.abox == ':error':
          print(self.evaluate('(|OWLAPI-getLastAnswer|)'))
          sys.exit(0)
      cloned.memory = copy.deepcopy(self.memory)

      KnowledgeBase.current_abox = cloned.abox
      cloned.prepare()

      return cloned

  def forget(self):
      KnowledgeBase.connection.writeline('(delete-abox {0})'.format(self.abox))
      KnowledgeBase.connection.flush()

  def cache(self, statement, result):
      self.memory[statement] = result
      return result

  def invalidate(self):
      self.memory.clear()

  def diff(self, other):
      return self.dump().difference(other.dump())

  # def get_concepts(self):
  #     return list(set(self.process(self.evaluate('(tbox-retrieve (?x ?y) (and (top ?x) (?x ?y has-child)))'))))
  #
  # def get_roles(self):
  #     return list(set(self.process(self.evaluate('(tbox-retrieve (?x ?y) (and (top ?x) (?x ?y has-child)))'))))

  def dump(self):
      facts = list()

      individuals = self.process(self.evaluate('(all-individuals)'))

      concepts = self.process(self.evaluate('(all-concept-assertions)'))
      offset = 0

      for n in range(len(concepts)):
          if n + offset >= len(concepts):
              break

          individual = concepts[n + offset]

          for i in range(1,len(concepts) - (n + offset)):
              if concepts[n+offset+i] == individual:
                  continue

              if concepts[n+offset+i] in individuals:
                  break

              facts.append('({0} {1})'.format(concepts[n+offset+(i)], individual))

          offset += i - 1

      roles = self.process(self.evaluate('(all-role-assertions)'))
      for n in range(0, len(roles), 3):
          facts.append('({0} {1} {2})'.format(roles[n+2],roles[n+1],roles[n]))

      return set(facts)

  def get_id(self):
    return self.abox

  def get_uri(self):
    return self.evaluate('(get-namespace-prefix {0}|)'.format(re.findall('.*.owl', self.get_id())[0]))[1:-1]

  def get_empty(self):
    cloned = KnowledgeBase()
    cloned.abox = self.evaluate('(clone-abox DEFAULT)')
    KnowledgeBase.current_abox = cloned.abox
    cloned.prepare()
    return cloned