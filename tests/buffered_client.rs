use mini_redis::{
    clients::{BufferedClient, Client},
    server,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Базовый тест. Экземпляр сервера запускается в фоновой задаче.
/// Затем создается экземпляр клиента, который используется для
/// инициализации буфера. Серверу отправляются команды `set` и `get`.
/// Затем оценивается ответ.
#[tokio::test]
async fn pool_key_value_get_set() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut client = BufferedClient::buffer(client);

    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    (addr, handle)
}
