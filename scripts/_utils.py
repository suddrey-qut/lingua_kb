
def logical_split(logical):
  def recursive_logical_split(tokens, layer = 0):
    token = tokens.pop(0)
    if '(' == token:
      L = []
      while tokens[0] != ')':
        L.append(recursive_logical_split(tokens, layer + 1))
      tokens.pop(0)

      if layer:
        return '(' + ' '.join(L) + ')'
      else:
        return L

    if layer > 0:
      return token

    return [token]
    
  tokens =  logical.replace('(', ' ( ').replace(')', ' ) ').split()
  return recursive_logical_split(tokens)


    
def is_negative(term):
  return term.startswith('(not ')

def is_complement(term):
  return term.startswith('(!')

def negate(term):
  if term.startswith('(not '):
    return logical_split(term)[1]
  return '(not ' + term + ')'