use opaque_ke::ciphersuite::CipherSuite;
use opaque_ke::{
    ClientLogin, ClientLoginFinishParameters, ClientRegistration,
    ClientRegistrationFinishParameters, CredentialFinalization, CredentialRequest,
    CredentialResponse, RegistrationRequest, RegistrationResponse, RegistrationUpload, ServerLogin,
    ServerLoginParameters, ServerRegistration, ServerSetup,
};
use rand_core::OsRng;

struct ProbeCipherSuite;

impl CipherSuite for ProbeCipherSuite {
    type OprfCs = opaque_ke::Ristretto255;
    type KeyExchange = opaque_ke::TripleDh<opaque_ke::Ristretto255, sha2::Sha512>;
    type Ksf = opaque_ke::ksf::Identity;
}

const CREDENTIAL_ID: &[u8] = b"drift-p2-04-probe";
const PAIRING_CODE: &[u8] = b"synthetic-p2-04-code";
const WRONG_CODE: &[u8] = b"synthetic-p2-04-wrong";

fn register_code(server_setup: &ServerSetup<ProbeCipherSuite>, client_rng: &mut OsRng) -> Vec<u8> {
    let registration_start =
        ClientRegistration::<ProbeCipherSuite>::start(client_rng, PAIRING_CODE).unwrap();
    let registration_request =
        RegistrationRequest::deserialize(&registration_start.message.serialize()).unwrap();
    let registration_response = ServerRegistration::<ProbeCipherSuite>::start(
        server_setup,
        registration_request,
        CREDENTIAL_ID,
    )
    .unwrap();
    let client_finish = registration_start
        .state
        .finish(
            client_rng,
            PAIRING_CODE,
            RegistrationResponse::deserialize(&registration_response.message.serialize()).unwrap(),
            ClientRegistrationFinishParameters::default(),
        )
        .unwrap();
    ServerRegistration::<ProbeCipherSuite>::finish(
        RegistrationUpload::deserialize(&client_finish.message.serialize()).unwrap(),
    )
    .serialize()
    .to_vec()
}

#[test]
fn opaque_ke_registration_and_login_share_session_key() {
    let mut server_rng = OsRng;
    let server_setup = ServerSetup::<ProbeCipherSuite>::new(&mut server_rng);
    let mut client_rng = OsRng;
    let password_file = register_code(&server_setup, &mut client_rng);

    let client_start =
        ClientLogin::<ProbeCipherSuite>::start(&mut client_rng, PAIRING_CODE).unwrap();
    let server_start = ServerLogin::start(
        &mut server_rng,
        &server_setup,
        Some(ServerRegistration::<ProbeCipherSuite>::deserialize(&password_file).unwrap()),
        CredentialRequest::deserialize(&client_start.message.serialize()).unwrap(),
        CREDENTIAL_ID,
        ServerLoginParameters::default(),
    )
    .unwrap();
    let client_finish = client_start
        .state
        .finish(
            &mut client_rng,
            PAIRING_CODE,
            CredentialResponse::deserialize(&server_start.message.serialize()).unwrap(),
            ClientLoginFinishParameters::default(),
        )
        .unwrap();
    let server_finish = server_start
        .state
        .finish(
            CredentialFinalization::deserialize(&client_finish.message.serialize()).unwrap(),
            ServerLoginParameters::default(),
        )
        .unwrap();

    assert_eq!(client_finish.session_key, server_finish.session_key);
    assert_eq!(client_finish.session_key.len(), 64);
}

#[test]
fn opaque_ke_rejects_wrong_pairing_code_without_returning_session_key() {
    let mut server_rng = OsRng;
    let server_setup = ServerSetup::<ProbeCipherSuite>::new(&mut server_rng);
    let mut registration_rng = OsRng;
    let password_file = register_code(&server_setup, &mut registration_rng);
    let mut client_rng = OsRng;

    let client_start = ClientLogin::<ProbeCipherSuite>::start(&mut client_rng, WRONG_CODE).unwrap();
    let server_start = ServerLogin::start(
        &mut server_rng,
        &server_setup,
        Some(ServerRegistration::<ProbeCipherSuite>::deserialize(&password_file).unwrap()),
        CredentialRequest::deserialize(&client_start.message.serialize()).unwrap(),
        CREDENTIAL_ID,
        ServerLoginParameters::default(),
    )
    .unwrap();
    let client_result = client_start.state.finish(
        &mut client_rng,
        WRONG_CODE,
        CredentialResponse::deserialize(&server_start.message.serialize()).unwrap(),
        ClientLoginFinishParameters::default(),
    );

    assert!(client_result.is_err());
}

#[test]
fn opaque_ke_client_message_is_bounded_and_does_not_echo_pairing_code() {
    let mut client_rng = OsRng;
    let client_start =
        ClientLogin::<ProbeCipherSuite>::start(&mut client_rng, PAIRING_CODE).unwrap();
    let message = client_start.message.serialize();

    assert!(message.len() <= 4 * 1024 * 1024);
    assert!(!message
        .windows(PAIRING_CODE.len())
        .any(|window| window == PAIRING_CODE));
}
