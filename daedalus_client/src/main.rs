<<<<<<< HEAD
use log::{debug, error, info, warn};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, Region};
=======
use crate::util::{
    format_url, upload_file_to_bucket, upload_url_to_bucket_mirrors,
    REQWEST_CLIENT,
};
use daedalus::get_path_from_artifact;
use dashmap::{DashMap, DashSet};
>>>>>>> modrinth/master
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod error;
mod fabric;
mod forge;
mod minecraft;
pub mod util;

<<<<<<< HEAD
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    DaedalusError(#[from] daedalus::Error),
    #[error("Error while deserializing JSON")]
    SerdeError(#[from] serde_json::Error),
    #[error("Error while deserializing XML")]
    XMLError(#[from] serde_xml_rs::Error),
    #[error("Unable to fetch {item}")]
    FetchError { inner: reqwest::Error, item: String },
    #[error("Error while managing asynchronous tasks")]
    TaskError(#[from] tokio::task::JoinError),
    #[error("Error while uploading file to S3")]
    S3Error { inner: S3Error, file: String },
    #[error("Error while parsing version as semver: {0}")]
    SemVerError(#[from] semver::Error),
    #[error("Error while reading zip file: {0}")]
    ZipError(#[from] zip::result::ZipError),
    #[error("Error while reading zip file: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Error while obtaining strong reference to Arc")]
    ArcError,
    #[error("Error acquiring semaphore: {0}")]
    AcquireError(#[from] tokio::sync::AcquireError),
    #[error("{0}")]
    TestError(String),
}
=======
pub use error::{Error, ErrorKind, Result};
>>>>>>> modrinth/master

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let subscriber = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .with(ErrorLayer::default());

    tracing::subscriber::set_global_default(subscriber)?;

    tracing::info!("Initialized tracing. Starting Daedalus!");

    if check_env_vars() {
        tracing::error!("Some environment variables are missing!");

        return Ok(());
    }

    let semaphore = Arc::new(Semaphore::new(
        dotenvy::var("CONCURRENCY_LIMIT")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or(10),
    ));

    // path, upload file
    let upload_files: DashMap<String, UploadFile> = DashMap::new();
    // path, mirror artifact
    let mirror_artifacts: DashMap<String, MirrorArtifact> = DashMap::new();

    minecraft::fetch(semaphore.clone(), &upload_files, &mirror_artifacts)
        .await?;
    fabric::fetch_fabric(semaphore.clone(), &upload_files, &mirror_artifacts)
        .await?;
    fabric::fetch_quilt(semaphore.clone(), &upload_files, &mirror_artifacts)
        .await?;
    forge::fetch_neo(semaphore.clone(), &upload_files, &mirror_artifacts)
        .await?;
    forge::fetch_forge(semaphore.clone(), &upload_files, &mirror_artifacts)
        .await?;

    futures::future::try_join_all(upload_files.iter().map(|x| {
        upload_file_to_bucket(
            x.key().clone(),
            x.value().file.clone(),
            x.value().content_type.clone(),
            &semaphore,
        )
    }))
    .await?;

    futures::future::try_join_all(mirror_artifacts.iter().map(|x| {
        upload_url_to_bucket_mirrors(
            format!("maven/{}", x.key()),
            x.value()
                .mirrors
                .iter()
                .map(|mirror| {
                    if mirror.entire_url {
                        mirror.path.clone()
                    } else {
                        format!("{}{}", mirror.path, x.key())
                    }
                })
                .collect(),
            x.sha1.clone(),
            &semaphore,
        )
    }))
    .await?;

    if dotenvy::var("CLOUDFLARE_INTEGRATION")
        .ok()
        .and_then(|x| x.parse::<bool>().ok())
        .unwrap_or(false)
    {
        if let Ok(token) = dotenvy::var("CLOUDFLARE_TOKEN") {
            if let Ok(zone_id) = dotenvy::var("CLOUDFLARE_ZONE_ID") {
                let cache_clears = upload_files
                    .into_iter()
                    .map(|x| format_url(&x.0))
                    .chain(
                        mirror_artifacts
                            .into_iter()
                            .map(|x| format_url(&format!("maven/{}", x.0))),
                    )
                    .collect::<Vec<_>>();

                // Cloudflare ratelimits cache clears to 500 files per request
                for chunk in cache_clears.chunks(500) {
                    REQWEST_CLIENT.post(format!("https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache"))
                        .bearer_auth(&token)
                        .json(&serde_json::json!({
                        "files": chunk
                    }))
                        .send()
                        .await
                        .map_err(|err| {
                            ErrorKind::Fetch {
                                inner: err,
                                item: "cloudflare clear cache".to_string(),
                            }
                        })?
                        .error_for_status()
                        .map_err(|err| {
                            ErrorKind::Fetch {
                                inner: err,
                                item: "cloudflare clear cache".to_string(),
                            }
                        })?;
                }
            }
<<<<<<< HEAD
        };

        if let Some(manifest) = versions {
            match forge::retrieve_data(
                &manifest,
                &mut uploaded_files,
                semaphore.clone(),
            )
            .await
            {
                Ok(..) => {}
                Err(err) => error!("{:?}", err),
            };

            match fabric::retrieve_data(
                &manifest,
                &mut uploaded_files,
                semaphore.clone(),
            )
            .await
            {
                Ok(..) => {}
                Err(err) => error!("{:?}", err),
            };

            match quilt::retrieve_data(
                &manifest,
                &mut uploaded_files,
                semaphore.clone(),
            )
            .await
            {
                Ok(..) => {}
                Err(err) => error!("{:?}", err),
            };

            match neo::retrieve_data(
                &manifest,
                &mut uploaded_files,
                semaphore.clone(),
            )
            .await
            {
                Ok(..) => {}
                Err(err) => error!("{:?}", err),
            };
=======
>>>>>>> modrinth/master
        }
    }

    Ok(())
}

pub struct UploadFile {
    file: bytes::Bytes,
    content_type: Option<String>,
}

pub struct MirrorArtifact {
    pub sha1: Option<String>,
    pub mirrors: DashSet<Mirror>,
}

#[derive(Eq, PartialEq, Hash)]
pub struct Mirror {
    path: String,
    entire_url: bool,
}

#[tracing::instrument(skip(mirror_artifacts))]
pub fn insert_mirrored_artifact(
    artifact: &str,
    sha1: Option<String>,
    mirrors: Vec<String>,
    entire_url: bool,
    mirror_artifacts: &DashMap<String, MirrorArtifact>,
) -> Result<()> {
    let mut val = mirror_artifacts
        .entry(get_path_from_artifact(artifact)?)
        .or_insert(MirrorArtifact {
            sha1,
            mirrors: DashSet::new(),
        });

    for mirror in mirrors {
        val.mirrors.insert(Mirror {
            path: mirror,
            entire_url,
        });
    }

    Ok(())
}

fn check_env_vars() -> bool {
    let mut failed = false;

    fn check_var<T: std::str::FromStr>(var: &str) -> bool {
        if dotenvy::var(var)
            .ok()
            .and_then(|s| s.parse::<T>().ok())
            .is_none()
        {
            tracing::warn!(
                "Variable `{}` missing in dotenvy or not of type `{}`",
                var,
                std::any::type_name::<T>()
            );
            true
        } else {
            false
        }
    }

    failed |= check_var::<String>("BASE_URL");

    failed |= check_var::<String>("S3_ACCESS_TOKEN");
    failed |= check_var::<String>("S3_SECRET");
    failed |= check_var::<String>("S3_URL");
    failed |= check_var::<String>("S3_REGION");
    failed |= check_var::<String>("S3_BUCKET_NAME");

    if dotenvy::var("CLOUDFLARE_INTEGRATION")
        .ok()
        .and_then(|x| x.parse::<bool>().ok())
        .unwrap_or(false)
    {
        failed |= check_var::<String>("CLOUDFLARE_TOKEN");
        failed |= check_var::<String>("CLOUDFLARE_ZONE_ID");
    }

    failed
}
<<<<<<< HEAD

lazy_static::lazy_static! {
    static ref CLIENT : Bucket = {
        let region = dotenvy::var("S3_REGION").unwrap();
        let b = Bucket::new(
            &dotenvy::var("S3_BUCKET_NAME").unwrap(),
            if &*region == "r2" {
                Region::R2 {
                    account_id: dotenvy::var("S3_URL").unwrap(),
                }
            } else {
                Region::Custom {
                    region: region.clone(),
                    endpoint: dotenvy::var("S3_URL").unwrap(),
                }
            },
            Credentials::new(
                Some(&*dotenvy::var("S3_ACCESS_TOKEN").unwrap()),
                Some(&*dotenvy::var("S3_SECRET").unwrap()),
                None,
                None,
                None,
            ).unwrap(),
        ).unwrap();

        if region == "path-style" {
            b.with_path_style()
        } else {
            b
        }
    };
}

pub async fn upload_file_to_bucket(
    path: String,
    bytes: Vec<u8>,
    content_type: Option<String>,
    uploaded_files: &tokio::sync::Mutex<Vec<String>>,
    semaphore: Arc<Semaphore>,
) -> Result<(), Error> {
    let _permit = semaphore.acquire().await?;
    info!("{} started uploading", path);
    let key = path.clone();

    for attempt in 1..=4 {
        let result = if let Some(ref content_type) = content_type {
            CLIENT
                .put_object_with_content_type(key.clone(), &bytes, content_type)
                .await
        } else {
            CLIENT.put_object(key.clone(), &bytes).await
        }
        .map_err(|err| Error::S3Error {
            inner: err,
            file: path.clone(),
        });

        match result {
            Ok(_) => {
                {
                    info!("{} done uploading", path);
                    let mut uploaded_files = uploaded_files.lock().await;
                    uploaded_files.push(key);
                }

                return Ok(());
            }
            Err(_) if attempt <= 3 => continue,
            Err(_) => {
                result?;
            }
        }
    }
    unreachable!()
}

pub fn format_url(path: &str) -> String {
    format!("{}/{}", &*dotenvy::var("BASE_URL").unwrap(), path)
}

pub async fn download_file(
    url: &str,
    sha1: Option<&str>,
    semaphore: Arc<Semaphore>,
) -> Result<bytes::Bytes, Error> {
    let _permit = semaphore.acquire().await?;
    info!("{} started downloading", url);
    let val = daedalus::download_file(url, sha1).await?;
    info!("{} finished downloading", url);
    // Err(Error::TestError("test error".to_owned()))
    Ok(val)
}

pub async fn download_file_mirrors(
    base: &str,
    mirrors: &[&str],
    sha1: Option<&str>,
    semaphore: Arc<Semaphore>,
) -> Result<bytes::Bytes, Error> {
    let _permit = semaphore.acquire().await?;
    info!("{} started downloading", base);
    let val = daedalus::download_file_mirrors(base, mirrors, sha1).await?;
    info!("{} finished downloading", base);

    Ok(val)
}
=======
>>>>>>> modrinth/master
