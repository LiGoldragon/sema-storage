use std::{env, path::PathBuf};

use sema_storage::{NegotiatedWire, Runtime};
use signal_sema_storage::{
    DocumentKey, DocumentKind, DocumentPayload, FamilyDeclaration, FixtureScope, FrameMessage,
    NameTableBytes, Reply, Request, SemaStorageRoot, SlotIdentifier, Version, Wire,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().unwrap_or_else(|| "help".into());
    if command == "daemon" {
        let socket = PathBuf::from(
            arguments
                .next()
                .unwrap_or_else(|| "/tmp/new-language-engine/sema.sock".into()),
        );
        let database = PathBuf::from(
            arguments
                .next()
                .unwrap_or_else(|| "/tmp/new-language-engine/state.sema".into()),
        );
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket)?;
        let runtime = Runtime::open(&database).await?;
        println!("READY {}", socket.display());
        loop {
            let (stream, _) = listener.accept().await?;
            let runtime = runtime.clone();
            tokio::spawn(async move {
                let _ = serve(stream, runtime).await;
            });
        }
    }

    let socket = PathBuf::from(arguments.next().ok_or("missing socket path")?);
    let request = match command.as_str() {
        "list" => Request::List {
            scope: FixtureScope(parse(&mut arguments, "scope")?),
            kind: arguments.next().map(|kind| parse_kind(&kind)).transpose()?,
        },
        "fetch" => Request::Fetch {
            key: DocumentKey {
                scope: FixtureScope(parse(&mut arguments, "scope")?),
                kind: parse_kind(&arguments.next().ok_or("kind")?)?,
                slot: SlotIdentifier(parse(&mut arguments, "slot")?),
            },
            version: arguments
                .next()
                .map(|value| value.parse().map(Version))
                .transpose()?,
        },
        "hash-fetch" => Request::HashFetch {
            hash: signal_sema_storage::ContentHash(parse_hash(
                &arguments.next().ok_or("hash")?,
            )?),
        },
        "snapshot" => Request::Snapshot {
            scope: FixtureScope(parse(&mut arguments, "scope")?),
        },
        "allocate" => Request::AllocateIdentifiers {
            scope: FixtureScope(parse(&mut arguments, "scope")?),
            count: parse(&mut arguments, "count")?,
        },
        "store-sema-family" => {
            let scope = FixtureScope(parse(&mut arguments, "scope")?);
            let slot = SlotIdentifier(parse(&mut arguments, "slot")?);
            let family = name_table::Identifier::Fixture(parse(&mut arguments, "family id")?);
            let layout_version = parse(&mut arguments, "layout version")?;
            Request::Store {
                key: DocumentKey {
                    scope,
                    kind: DocumentKind::SemaStorage,
                    slot,
                },
                payload: DocumentPayload::SemaStorage(SemaStorageRoot {
                    families: vec![FamilyDeclaration {
                        family,
                        layout_version,
                    }],
                    names: NameTableBytes(Vec::new()),
                }),
            }
        }
        "subscribe" => {
            let request = Request::Subscribe {
                scope: FixtureScope(parse(&mut arguments, "scope")?),
                kind: arguments.next().map(|kind| parse_kind(&kind)).transpose()?,
            };
            return subscribe(&socket, request).await;
        }
        _ => return Err("usage: sema-storage daemon [socket] [database] | list|fetch|hash-fetch|snapshot|allocate|store-sema-family|subscribe <socket> ...".into()),
    };
    println!("{:?}", exchange(&socket, &request).await?);
    Ok(())
}

fn parse<T: std::str::FromStr>(
    arguments: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T::Err: std::error::Error + 'static,
{
    Ok(arguments
        .next()
        .ok_or_else(|| format!("missing {name}"))?
        .parse()?)
}

fn parse_kind(value: &str) -> Result<DocumentKind, Box<dyn std::error::Error>> {
    Ok(match value {
        "type-schema" => DocumentKind::TypeSchema,
        "signal-contract" => DocumentKind::SignalContract,
        "nexus-runtime" => DocumentKind::NexusRuntime,
        "sema-storage" => DocumentKind::SemaStorage,
        "nomos" => DocumentKind::Nomos,
        "logos" => DocumentKind::Logos,
        _ => return Err(format!("unknown document kind: {value}").into()),
    })
}

fn parse_hash(value: &str) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    if value.len() != 64 {
        return Err("hash must contain 64 hexadecimal digits".into());
    }
    let mut output = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(output)
}

struct FramedSocket {
    stream: UnixStream,
    sequence: u64,
}
impl FramedSocket {
    async fn connect(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let mut socket = Self {
            stream: UnixStream::connect(path).await?,
            sequence: 0,
        };
        socket
            .stream
            .write_all(&Wire::frame_current_handshake_request()?)
            .await?;
        if !Wire::decode_frame(&socket.read_frame().await?)?.is_accepted_handshake() {
            return Err("daemon rejected shared frame protocol".into());
        }
        Ok(socket)
    }

    async fn accept(
        stream: UnixStream,
    ) -> Result<(Self, NegotiatedWire), Box<dyn std::error::Error>> {
        let mut socket = Self {
            stream,
            sequence: 0,
        };
        let FrameMessage::HandshakeRequest(peer) = Wire::decode_frame(&socket.read_frame().await?)?
        else {
            return Err("first frame was not a protocol handshake".into());
        };
        socket
            .stream
            .write_all(&Wire::frame_handshake_reply(Wire::handshake_reply(peer))?)
            .await?;
        Ok((socket, NegotiatedWire::new(peer)))
    }

    async fn request(&mut self, request: &Request) -> Result<(), Box<dyn std::error::Error>> {
        let payload = Wire::encode_request(request)?;
        let frame = Wire::frame_request(payload, self.sequence)?;
        self.sequence += 1;
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    async fn reply(&mut self) -> Result<Reply, Box<dyn std::error::Error>> {
        let FrameMessage::Reply { payload, .. } = Wire::decode_frame(&self.read_frame().await?)?
        else {
            return Err("expected shared reply frame".into());
        };
        Ok(rkyv::from_bytes::<Reply, rkyv::rancor::Error>(&payload)?)
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let length = self.stream.read_u32().await? as usize;
        if length > 16 * 1024 * 1024 {
            return Err("frame too large".into());
        }
        let mut frame = Vec::with_capacity(length + 4);
        frame.extend_from_slice(&(length as u32).to_be_bytes());
        frame.resize(length + 4, 0);
        self.stream.read_exact(&mut frame[4..]).await?;
        Ok(frame)
    }
}

async fn exchange(
    socket: &PathBuf,
    request: &Request,
) -> Result<Reply, Box<dyn std::error::Error>> {
    let mut socket = FramedSocket::connect(socket).await?;
    socket.request(request).await?;
    socket.reply().await
}

async fn subscribe(socket: &PathBuf, request: Request) -> Result<(), Box<dyn std::error::Error>> {
    let mut socket = FramedSocket::connect(socket).await?;
    socket.request(&request).await?;
    loop {
        println!("{:?}", socket.reply().await?);
    }
}

async fn serve(stream: UnixStream, runtime: Runtime) -> Result<(), Box<dyn std::error::Error>> {
    let (mut socket, negotiated) = FramedSocket::accept(stream).await?;
    let FrameMessage::Request { exchange, payload } =
        Wire::decode_frame(&socket.read_frame().await?)?
    else {
        return Err("expected shared request frame".into());
    };
    if let Some(rejection) = negotiated.request_rejection() {
        socket
            .stream
            .write_all(&Wire::frame_reply(
                exchange,
                Wire::encode_reply(&Reply::Rejected(rejection))?,
            )?)
            .await?;
        return Ok(());
    }
    let request = rkyv::from_bytes::<Request, rkyv::rancor::Error>(&payload)?;
    let subscription_filter = match &request {
        Request::Subscribe { scope, kind } => Some((*scope, *kind)),
        _ => None,
    };
    let mut events = runtime.subscribe();
    let reply = runtime.request(request).await?;
    socket
        .stream
        .write_all(&Wire::frame_reply(exchange, Wire::encode_reply(&reply)?)?)
        .await?;
    if let Some((scope, kind)) = subscription_filter {
        while let Ok(event) = events.recv().await {
            if event.document.key.scope == scope
                && kind.is_none_or(|expected| event.document.key.kind == expected)
            {
                socket
                    .stream
                    .write_all(&Wire::frame_reply(
                        exchange,
                        Wire::encode_reply(&Reply::Event(event))?,
                    )?)
                    .await?;
            }
        }
    }
    Ok(())
}
