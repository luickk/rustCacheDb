# rustCacheDb
blazing fast temporary key/val storage

`uint8_t opCode(pull=1, push=2, pullReply=3) - uint16_t (query)keySize - char[] (query)key - uint16_t(val) valSize - char[] val`
