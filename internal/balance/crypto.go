package balance

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

type SecretBox struct {
	aead cipher.AEAD
}

func NewSecretBox(key []byte) (*SecretBox, error) {
	if len(key) != 32 {
		return nil, errors.New("上游凭据加密密钥未配置或长度无效")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &SecretBox{aead: aead}, nil
}

func (b *SecretBox) Encrypt(plaintext []byte) ([]byte, []byte, error) {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	return nonce, b.aead.Seal(nil, nonce, plaintext, nil), nil
}

func (b *SecretBox) Decrypt(nonce, ciphertext []byte) ([]byte, error) {
	if len(nonce) != b.aead.NonceSize() || len(ciphertext) == 0 {
		return nil, errors.New("加密凭据数据无效")
	}
	return b.aead.Open(nil, nonce, ciphertext, nil)
}
