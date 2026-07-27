use std::io;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};

/// The handshake each peer sends to prove it controls the private key behind
/// its advertised public key. The signature is made over the *other* peer's
/// public key, so it cannot be replayed against a third party.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeMessage {
    pub public_key: String,
    pub signature: String,
}

/// Anything that can go wrong while building or verifying a handshake. Kept
/// as data (no panics) so the networking code can log and reject malformed
/// input instead of crashing the task.
#[derive(Debug)]
pub enum HandshakeError {
    InvalidPublicKeyEncoding(base64::DecodeError),
    InvalidSignatureEncoding(base64::DecodeError),
    WrongPublicKeyLength { expected: usize, found: usize },
    WrongSignatureLength { expected: usize, found: usize },
    InvalidPublicKey(ed25519_dalek::SignatureError),
    SignatureVerificationFailed,
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::InvalidPublicKeyEncoding(error) => {
                write!(formatter, "public key is not valid base64: {error}")
            }
            HandshakeError::InvalidSignatureEncoding(error) => {
                write!(formatter, "signature is not valid base64: {error}")
            }
            HandshakeError::WrongPublicKeyLength { expected, found } => write!(
                formatter,
                "public key has wrong length: expected {expected} bytes, found {found}"
            ),
            HandshakeError::WrongSignatureLength { expected, found } => write!(
                formatter,
                "signature has wrong length: expected {expected} bytes, found {found}"
            ),
            HandshakeError::InvalidPublicKey(error) => {
                write!(formatter, "public key is not a valid ed25519 key: {error}")
            }
            HandshakeError::SignatureVerificationFailed => {
                write!(formatter, "signature verification failed")
            }
        }
    }
}

impl std::error::Error for HandshakeError {}

/// This machine's long-lived cryptographic identity: an ed25519 keypair whose
/// public half is shared with peers and whose private half never leaves disk.
pub struct Identity {
    signing_key: SigningKey,
}

impl Identity {
    /// Create a fresh random identity. Does not touch disk; call [`save`] to
    /// persist it.
    ///
    /// [`save`]: Identity::save
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        OsRng.fill_bytes(&mut seed);
        Identity {
            signing_key: SigningKey::from_bytes(&seed),
        }
    }

    /// Load an identity previously written by [`save`]. The file holds the
    /// base64-encoded 32-byte ed25519 seed.
    ///
    /// [`save`]: Identity::save
    pub fn load(path: &Path) -> io::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let bytes = BASE64.decode(contents.trim()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "identity key at {} is not valid base64: {error}",
                    path.display()
                ),
            )
        })?;
        let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "identity key at {} decoded to {} bytes, expected 32",
                    path.display(),
                    bytes.len()
                ),
            )
        })?;
        Ok(Identity {
            signing_key: SigningKey::from_bytes(&seed),
        })
    }

    /// Persist this identity to `path` as the base64-encoded 32-byte ed25519
    /// seed. Refuses to overwrite an existing file so an accidental keygen
    /// can't silently rotate (and thus invalidate) the machine's identity.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        use std::io::Write;
        file.write_all(BASE64.encode(self.signing_key.to_bytes()).as_bytes())
    }

    /// The base64-encoded public key advertised to peers and stored in config.
    pub fn public_key(&self) -> String {
        BASE64.encode(self.signing_key.verifying_key().to_bytes())
    }

    /// Build our half of the handshake, proving ownership of our private key
    /// by signing the peer's public key.
    pub fn sign_handshake(
        &self,
        peer_public_key: &str,
    ) -> Result<HandshakeMessage, HandshakeError> {
        let peer_public_key_bytes = BASE64
            .decode(peer_public_key)
            .map_err(HandshakeError::InvalidPublicKeyEncoding)?;
        let signature = self.signing_key.sign(&peer_public_key_bytes);
        Ok(HandshakeMessage {
            public_key: self.public_key(),
            signature: BASE64.encode(signature.to_bytes()),
        })
    }

    /// Verify a handshake received from a peer. Confirms the peer signed *our*
    /// public key with the private key matching their advertised public key.
    ///
    /// On success returns the peer's verified public key (base64). Never
    /// panics on malformed input; every failure mode is a [`HandshakeError`].
    pub fn verify_handshake(&self, message: &HandshakeMessage) -> Result<String, HandshakeError> {
        let peer_public_key_bytes = BASE64
            .decode(&message.public_key)
            .map_err(HandshakeError::InvalidPublicKeyEncoding)?;
        let peer_public_key_array: [u8; 32] =
            peer_public_key_bytes.as_slice().try_into().map_err(|_| {
                HandshakeError::WrongPublicKeyLength {
                    expected: 32,
                    found: peer_public_key_bytes.len(),
                }
            })?;
        let peer_verifying_key = VerifyingKey::from_bytes(&peer_public_key_array)
            .map_err(HandshakeError::InvalidPublicKey)?;

        let signature_bytes = BASE64
            .decode(&message.signature)
            .map_err(HandshakeError::InvalidSignatureEncoding)?;
        let signature_array: [u8; 64] = signature_bytes.as_slice().try_into().map_err(|_| {
            HandshakeError::WrongSignatureLength {
                expected: 64,
                found: signature_bytes.len(),
            }
        })?;
        let signature = Signature::from_bytes(&signature_array);

        let our_public_key_bytes = self.signing_key.verifying_key().to_bytes();
        peer_verifying_key
            .verify(&our_public_key_bytes, &signature)
            .map_err(|_| HandshakeError::SignatureVerificationFailed)?;

        Ok(message.public_key.clone())
    }
}
