use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes256Gcm, Nonce as AesNonce,
};
use chacha20poly1305::{ChaCha20Poly1305, Nonce as ChaChaNonce};
use drift_protocol::WireLimits;
use hkdf::Hkdf;
use sha2::Sha256;
use std::fmt;

const MAX_FRAME_PAYLOAD: usize = 4 * 1024 * 1024;
const AEAD_TAG_LEN: usize = 16;

struct TestSecret([u8; 32]);

impl fmt::Debug for TestSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TestSecret([REDACTED])")
    }
}

#[test]
fn hkdf_sha256_matches_rfc5869_test_case_one() {
    let input_key_material = [0x0b; 22];
    let salt = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
    ];
    let info = [0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9];
    let mut output = [0_u8; 42];

    Hkdf::<Sha256>::new(Some(&salt), &input_key_material)
        .expand(&info, &mut output)
        .unwrap();

    assert_eq!(
        output,
        [
            0x3c, 0xb2, 0x5f, 0x25, 0xfa, 0xac, 0xd5, 0x7a, 0x90, 0x43, 0x4f, 0x64, 0xd0, 0x36,
            0x2f, 0x2a, 0x2d, 0x2d, 0x0a, 0x90, 0xcf, 0x1a, 0x5a, 0x4c, 0x5d, 0xb0, 0x2d, 0x56,
            0xec, 0xc4, 0xc5, 0xbf, 0x34, 0x00, 0x72, 0x08, 0xd5, 0xb8, 0x87, 0x18, 0x58, 0x65,
        ]
    );
}

#[test]
fn aes_gcm_supports_empty_payload_and_authenticates_associated_data() {
    let key = [0x42_u8; 32];
    let nonce = [0x24_u8; 12];
    let associated_data = b"drift-p2-04-aad";
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let mut ciphertext = Vec::new();
    let tag = cipher
        .encrypt_in_place_detached(
            AesNonce::from_slice(&nonce),
            associated_data,
            &mut ciphertext,
        )
        .unwrap();

    cipher
        .decrypt_in_place_detached(
            AesNonce::from_slice(&nonce),
            associated_data,
            &mut ciphertext,
            &tag,
        )
        .unwrap();
    assert!(ciphertext.is_empty());

    let mut tampered_aad = associated_data.to_vec();
    tampered_aad[0] ^= 1;
    assert!(cipher
        .decrypt_in_place_detached(
            AesNonce::from_slice(&nonce),
            &tampered_aad,
            &mut ciphertext,
            &tag,
        )
        .is_err());
}

#[test]
fn chacha20_poly1305_rejects_ciphertext_mutation() {
    let key = [0x51_u8; 32];
    let nonce = [0x61_u8; 12];
    let associated_data = b"drift-p2-04-context";
    let cipher = ChaCha20Poly1305::new_from_slice(&key).unwrap();
    let mut ciphertext = b"bounded secret fixture".to_vec();
    let tag = cipher
        .encrypt_in_place_detached(
            ChaChaNonce::from_slice(&nonce),
            associated_data,
            &mut ciphertext,
        )
        .unwrap();
    ciphertext[0] ^= 1;

    assert!(cipher
        .decrypt_in_place_detached(
            ChaChaNonce::from_slice(&nonce),
            associated_data,
            &mut ciphertext,
            &tag,
        )
        .is_err());
}

#[test]
fn secret_debug_is_redacted() {
    let secret = TestSecret([0x7a; 32]);
    let debug = format!("{secret:?}");

    assert_eq!(secret.0, [0x7a; 32]);
    assert_eq!(debug, "TestSecret([REDACTED])");
    assert!(!debug.contains("7a"));
}

#[test]
fn encrypted_record_payload_leaves_room_for_authentication_tag() {
    let maximum_plaintext = WireLimits::default().max_frame_payload - AEAD_TAG_LEN;

    assert_eq!(maximum_plaintext, MAX_FRAME_PAYLOAD - AEAD_TAG_LEN);
}
