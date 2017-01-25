
# encoding=UTF-8
'''
For when you've had enough of this shit and just want text to not blow up
- Shamelessly stolen from my first mentor James Hayden - 
'''

import unicodedata
import re

'''
Table of explicit conversions to perform prior to applying the standard transform
(This is useful for characters that should result in multicharacter representations
upon translation, as they will be discarded by the standard routine we're going to use.)
'''
explicit_mapping = {
    0x93:'"',   0x94:'"',
    0xc0:u'À',   0xc1:u'Á',   0xc2:u'Â',   0xc3:u'Ã',   0xc4:u'Ä',   0xc5:u'Ä',
    0xc6:u'Æ',  0xc7:u'Ç',
    0xc8:u'È',   0xc9:u'É',   0xca:u'Ê',   0xcb:u'Ë',
    0xcc:u'Ì',   0xcd:u'Í',   0xce:u'Î',   0xcf:u'Ï',
    0xd0:'Th',  0xd1:u'Ñ',
    0xd2:u'Ó',   0xd3:u'Ó',   0xd4:u'Ô',   0xd5:u'Õ',   0xd6:u'Ö',   0xd8:u'Ø',
    0xd9:u'Ù',   0xda:u'Ú',   0xdb:u'Û',   0xdc:u'Ü',
    0xdd:u'Ý',   0xde:'th',  0xdf:'ss',
    0xe0:u'à',   0xe1:u'á',   0xe2:u'â',   0xe3:u'ã',   0xe4:u'ä',   0xe5:u'å',
    0xe6:u'æ',  0xe7:u'ç',
    0xe8:u'è',   0xe9:u'é',   0xea:u'ê',   0xeb:u'ë',
    0xec:u'ì',   0xed:u'í',   0xee:u'î',   0xef:u'ï',
    0xf0:'th',  0xf1:u'ñ',
    0xf2:u'ò',   0xf3:u'ó',   0xf4:u'ô',   0xf5:u'õ',   0xf6:u'ö',   0xf8:u'ø',
    0xf9:u'ù',   0xfa:u'ú',   0xfb:u'û',   0xfc:u'ü',
    0xfd:u'ý',   0xfe:'th',  0xff:u'ÿ',
    0xa1:'!', 0xa2:'{cent}', 0xa3:'{pound}', 0xa4:'{currency}',
    0xa5:'{yen}', 0xa6:'|', 0xa7:'{section}', 0xa8:'{umlaut}',
    0xa9:u'©', 0xaa:'{^a}', 0xab:'<<', 0xac:'{not}',
    0xad:'-', 0xae:'{R}', 0xaf:'_', 0xb0:'{degrees}',
    0xb1:'{+/-}', 0xb2:'{^2}', 0xb3:'{^3}', 0xb4:"'",
    0xb5:'{micro}', 0xb6:'{paragraph}', 0xb7:'*', 0xb8:'{cedilla}',
    0xb9:'{^1}', 0xba:'{^o}', 0xbb:'>>', 
    0xbc:'{1/4}', 0xbd:'{1/2}', 0xbe:'{3/4}', 0xbf:'?',
    0xd7:'*', 0xf7:'/',
    }

'''The matcher that decides whether text is 0-127 or outside that'''
mapper = re.compile(u'([\x00-\x7f]+)|([^\x00-\x7f])', re.UNICODE).sub
#import pdb; pdb.set_trace()
def ufyl(x):
    if x.group(1):
        return x.group(1)
    else:
        return explicit_mapping.get(ord(x.group(2)), x.group(2))


def pretreat(raw_text):
    '''
    Apply the pre_mapping to explicitly translate the text, using the nonascii_matcher for improved performance
    @note: The submitted string is not modified; work is performed on a copy
    @return: The mapped copy of the string
    '''
    # Use nonascii_matcher to perform character substitution
    # This goes for the performance in the regex lib at the expense of code readability, so here's what it does.
    # nonascii_matcher returns two groups: one containing any text that's "normal" (for our purposes), and the other
    # containing text that didn't meet that definition.  The or operator short-circuits, returning the first thing that
    # has a bool equivalent of true.  So if some text is normal, it shows up in the first group returned by the matcher
    # and is therefore returned unmapped by the lambda function.  Text outside our desired range shows up in the 2nd
    # group, which we're mapping before evaluating in the or statement.  We use setdefault to map it so that characters
    # without a mapping will just map to themselves.
    #print 'Original', raw_text
    #return mapper(lambda x: x.group(1) or explicit_mapping.get(ord(x.group(2)), x.group(2)), raw_text)
    return mapper(ufyl, raw_text)

def sanitize(raw_text, apply_pre_mapping = True, write_line = None, encoding_mode='ignore'):
    '''
    Attempt to remove all non-7-bit-printable-ASCII characters
    by mapping to similar equivalents and ignoring all others
    @param apply_pre_mapping: Whether to map characters using explicit_mapping prior to using NFKD normalization
    '''
    # If no logging callback was specified, use a null logger
    if not write_line:
        write_line = lambda message: None
    
    # First, perform any explicit mappings, if requested
    if apply_pre_mapping:
        converted_text = pretreat(raw_text)
    else:
        converted_text = raw_text
    
    # Second, apply a standard normalization
    try:
        nkfd_form = unicodedata.normalize('NFKD', unicode(converted_text))
        converted_text = ''.join(c for c in nkfd_form if not unicodedata.combining(c))
    except Exception, e:
        write_line(['ERROR', 'Unable to convert some text, "%s"' % e])
    
    # Third, take anything that didn't end up as ASCII and get rid of it
    try:
        # To do this, first try to convert to ASCII, ignoring things that don't play nice
        converted_text = converted_text.encode('ascii', encoding_mode)
    except Exception:
        pass # Oh, well, we'll super-squash it next
    
    # Even ASCII conversion falls flat on its face sometimes, and we really, *really* just want to get rid of the
    # bad characters, so we'll just explicitly filter out everything outside our preferred range
    converted_text = ''.join(filter(lambda c: ord(c) in [10, 13] or (ord(c) >= 32 and ord(c) <= 127), converted_text))
    
    # We're done.  Send back the boring version of the string.
    return converted_text



