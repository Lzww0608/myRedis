use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    // 监听指定地址，等待TCP连接进来
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        // 第二个被忽略的项中包含有新连接的`IP`和端口信息
        let (socket, _) = listener.accept().await.unwrap();
        // 为每一条连接都生成一个新的任务，
        // `socket`的所有权将被移动到新的任务重，并在那里进行处理
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    use mini_redis::Command::{self, Get, Set};
    use std::collections::HashMap;

    // 使用hashmap存储Redis数据
    let mut db = HashMap::new();

    // `Connection`对于Redis的读写进行了抽象封装，因此我们读到的事一个一个数据帧frame(数据帧 = Redis命令 + 数据)，而不是字节流
    // `Connection`是在mini-redis中定义
    let mut connection = Connection::new(socket);

    // 使用`read_frame`方法从连接获取一个数据帧：一条Redis命令 + 相应的数据
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                // 值被存储为 `Vec<u8>`的形式
                db.insert(cmd.key().to_string(), cmd.value().to_vec());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    // `Frame::Bulk`期待数据的类型是`Bytes`
                    // `&Vec<u8>`可以使用`into()`方法转换成`Bytes`类型
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented command: {:?}", cmd),
        };
        //将请求相应返回给客户端
        connection.write_frame(&response).await.unwrap();
    }
}