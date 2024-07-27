use crate::util::{
    download_file, fetch_json, fetch_xml, format_url, sha1_async,
};
use crate::{insert_mirrored_artifact, Error, MirrorArtifact, UploadFile};
use chrono::{DateTime, Utc};
<<<<<<< HEAD
use daedalus::minecraft::{
    Argument, ArgumentType, Library, VersionManifest, VersionType,
};
use daedalus::modded::{
    LoaderVersion, Manifest, PartialVersionInfo, Processor, SidedDataEntry,
};
use lazy_static::lazy_static;
use log::{error, info};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
=======
use daedalus::get_path_from_artifact;
use daedalus::modded::PartialVersionInfo;
use dashmap::DashMap;
use futures::io::Cursor;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Deserialize;
>>>>>>> modrinth/master
use std::collections::HashMap;
use std::sync::Arc;
<<<<<<< HEAD
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;
=======
use tokio::sync::Semaphore;
>>>>>>> modrinth/master

#[tracing::instrument(skip(semaphore, upload_files, mirror_artifacts))]
pub async fn fetch_forge(
    semaphore: Arc<Semaphore>,
    upload_files: &DashMap<String, UploadFile>,
    mirror_artifacts: &DashMap<String, MirrorArtifact>,
) -> Result<(), Error> {
    let forge_manifest = fetch_json::<IndexMap<String, Vec<String>>>(
        "https://files.minecraftforge.net/net/minecraftforge/forge/maven-metadata.json",
        &semaphore,
    )
        .await?;

    let mut format_version = 0;

    let forge_versions = forge_manifest.into_iter().flat_map(|(game_version, versions)| versions.into_iter().map(|loader_version| {
        // Forge versions can be in these specific formats:
        // 1.10.2-12.18.1.2016-failtests
        // 1.9-12.16.0.1886
        // 1.9-12.16.0.1880-1.9
        // 1.14.4-28.1.30
        // This parses them to get the actual Forge version. Ex: 1.15.2-31.1.87 -> 31.1.87
        let version_split = loader_version.split('-').nth(1).unwrap_or(&loader_version).to_string();

        // Forge has 3 installer formats:
        // - Format 0 (Unsupported ATM): Forge Legacy (pre-1.5.2). Uses Binary Patch method to install
        //   To install: Download patch, download minecraft client JAR. Combine patch and client JAR and delete META-INF/.
        //      (pre-1.3-2) Client URL: https://maven.minecraftforge.net/net/minecraftforge/forge/{version}/forge-{version}-client.zip
        //      (pre-1.3-2) Server URL: https://maven.minecraftforge.net/net/minecraftforge/forge/{version}/forge-{version}-server.zip
        //      (1.3-2-onwards) Universal URL: https://maven.minecraftforge.net/net/minecraftforge/forge/{version}/forge-{version}-universal.zip
        // - Format 1: Forge Installer Legacy (1.5.2-1.12.2ish)
        //     To install: Extract install_profile.json from archive. "versionInfo" is the profile's version info. Convert it to the modern format
        //     Extract forge library from archive. Path is at "install"."path".
        // - Format 2: Forge Installer Modern
        //     To install: Extract install_profile.json from archive. Extract version.json from archive. Combine the two and extract all libraries
        //     which are embedded into the installer JAR.
        //     Then upload. The launcher will need to run processors!
        if format_version != 1 && &*version_split == "7.8.0.684" {
            format_version = 1;
        } else if format_version != 2 && &*version_split == "14.23.5.2851" {
            format_version = 2;
        }

        ForgeVersion {
            format_version,
            installer_url: format!("https://maven.minecraftforge.net/net/minecraftforge/forge/{0}/forge-{0}-installer.jar", loader_version),
            raw: loader_version,
            loader_version: version_split,
            game_version: game_version.clone(),
        }
    })
        .collect::<Vec<_>>())
        // TODO: support format version 0 (see above)
        .filter(|x| x.format_version != 0)
        .filter(|x| {
            // These following Forge versions are broken and cannot be installed
            const BLACKLIST : &[&str] = &[
                // Not supported due to `data` field being `[]` even though the type is a map
                "1.12.2-14.23.5.2851",
                // Malformed Archives
                "1.6.1-8.9.0.749",
                "1.6.1-8.9.0.751",
                "1.6.4-9.11.1.960",
                "1.6.4-9.11.1.961",
                "1.6.4-9.11.1.963",
                "1.6.4-9.11.1.964",
            ];

            !BLACKLIST.contains(&&*x.raw)
        })
        .collect::<Vec<_>>();

    fetch(
        daedalus::modded::CURRENT_FORGE_FORMAT_VERSION,
        "forge",
        "https://maven.minecraftforge.net/",
        forge_versions,
        semaphore,
        upload_files,
        mirror_artifacts,
    )
    .await
}

#[tracing::instrument(skip(semaphore, upload_files, mirror_artifacts))]
pub async fn fetch_neo(
    semaphore: Arc<Semaphore>,
    upload_files: &DashMap<String, UploadFile>,
    mirror_artifacts: &DashMap<String, MirrorArtifact>,
) -> Result<(), Error> {
    #[derive(Debug, Deserialize)]
    struct Metadata {
        versioning: Versioning,
    }

    #[derive(Debug, Deserialize)]
    struct Versioning {
        versions: Versions,
    }

    #[derive(Debug, Deserialize)]
    struct Versions {
        version: Vec<String>,
    }

    let forge_versions = fetch_xml::<Metadata>(
        "https://maven.neoforged.net/net/neoforged/forge/maven-metadata.xml",
        &semaphore,
    )
    .await?;
    let neo_versions = fetch_xml::<Metadata>(
        "https://maven.neoforged.net/net/neoforged/neoforge/maven-metadata.xml",
        &semaphore,
    )
    .await?;

    let parsed_versions = forge_versions.versioning.versions.version.into_iter().map(|loader_version| {
        // NeoForge Forge versions can be in these specific formats:
        // 1.20.1-47.1.74
        // 47.1.82
        // This parses them to get the actual Forge version. Ex: 1.20.1-47.1.74 -> 47.1.74
        let version_split = loader_version.split('-').nth(1).unwrap_or(&loader_version).to_string();

        Ok(ForgeVersion {
            format_version: 2,
            installer_url: format!("https://maven.neoforged.net/net/neoforged/forge/{0}/forge-{0}-installer.jar", loader_version),
            raw: loader_version,
            loader_version: version_split,
            game_version: "1.20.1".to_string(), // All NeoForge Forge versions are for 1.20.1
        })
    }).chain(neo_versions.versioning.versions.version.into_iter().map(|loader_version| {
        let mut parts = loader_version.split('.');

        // NeoForge Forge versions are in this format: 20.2.29-beta, 20.6.119
        // Where the first number is the major MC version, the second is the minor MC version, and the third is the NeoForge version
        let major = parts.next().ok_or_else(
            || crate::ErrorKind::InvalidInput(format!("Unable to find major game version for NeoForge {loader_version}"))
        )?;

        let minor = parts.next().ok_or_else(
            || crate::ErrorKind::InvalidInput(format!("Unable to find minor game version for NeoForge {loader_version}"))
        )?;

        let game_version = if minor == "0" {
            format!("1.{major}")
        } else {
            format!("1.{major}.{minor}")
        };

        Ok(ForgeVersion {
            format_version: 2,
            installer_url: format!("https://maven.neoforged.net/net/neoforged/neoforge/{0}/neoforge-{0}-installer.jar", loader_version),
            loader_version: loader_version.clone(),
            raw: loader_version,
            game_version,
        })
    }))
        .collect::<Result<Vec<_>, Error>>()?
        .into_iter()
        .filter(|x| {
        // These following Forge versions are broken and cannot be installed
        const BLACKLIST : &[&str] = &[
            // Unreachable / 404
            "1.20.1-47.1.7",
            "47.1.82",
        ];

        !BLACKLIST.contains(&&*x.raw)
    }).collect();

    fetch(
        daedalus::modded::CURRENT_NEOFORGE_FORMAT_VERSION,
        "neo",
        "https://maven.neoforged.net/",
        parsed_versions,
        semaphore,
        upload_files,
        mirror_artifacts,
    )
    .await
}

#[tracing::instrument(skip(
    forge_versions,
    semaphore,
    upload_files,
    mirror_artifacts
))]
async fn fetch(
    format_version: usize,
    mod_loader: &str,
    maven_url: &str,
    forge_versions: Vec<ForgeVersion>,
    semaphore: Arc<Semaphore>,
    upload_files: &DashMap<String, UploadFile>,
    mirror_artifacts: &DashMap<String, MirrorArtifact>,
) -> Result<(), Error> {
    let modrinth_manifest = fetch_json::<daedalus::modded::Manifest>(
        &format_url(&format!("{mod_loader}/v{format_version}/manifest.json",)),
        &semaphore,
    )
    .await
    .ok();

    let fetch_versions = if let Some(modrinth_manifest) = modrinth_manifest {
        let mut fetch_versions = Vec::new();

        for version in &forge_versions {
            if !modrinth_manifest.game_versions.iter().any(|x| {
                x.id == version.game_version
                    && x.loaders.iter().any(|x| x.id == version.loader_version)
            }) {
                fetch_versions.push(version);
            }
        }

<<<<<<< HEAD
        if !loaders.is_empty() {
            version_futures.push(async {
                let mut loaders_versions = Vec::new();

                {
                    let loaders_futures = loaders.into_iter().map(|(loader_version_full, version)| async {

                        // 1.11.2-13.20.0.2200
                        // println!("{:#?}", loader_version_full);

                        // Version {
                        //     major: 20,
                        //     minor: 0,
                        //     patch: 2200,
                        // }
                        // println!("{:#?}", version);

                        let versions_mutex = Arc::clone(&old_versions);
                        let visited_assets = Arc::clone(&visited_assets_mutex);
                        let uploaded_files_mutex = Arc::clone(&uploaded_files_mutex);
                        let semaphore = Arc::clone(&semaphore);
                        let minecraft_version = minecraft_version.clone();

                        // println!("versions_mutex: {:#?}", versions_mutex.lock().await);
                        // println!("visited_assets: {:#?}", visited_assets.lock().await);
                        // println!("uploaded_files_mutex: {:#?}", uploaded_files_mutex.lock().await);
                        // println!("semaphore: {:#?}", semaphore);
                        // println!("minecraft_version: {:#?}", minecraft_version);

                        async move {
                            /// These forge versions are not worth supporting!
                            const WHITELIST : &[&str] = &[
                                // Not supported due to `data` field being `[]` even though the type is a map
                                "1.12.2-14.23.5.2851",
                                // Malformed Archives
                                "1.6.1-8.9.0.749",
                                "1.6.1-8.9.0.751",
                                "1.6.4-9.11.1.960",
                                "1.6.4-9.11.1.961",
                                "1.6.4-9.11.1.963",
                                "1.6.4-9.11.1.964",
                            ];

                            if WHITELIST.contains(&&*loader_version_full) {
                                return Ok(None);
                            }

                            {
                                let versions = versions_mutex.lock().await;
                                let version = versions.iter().find(|x|
                                    x.id == minecraft_version).and_then(|x| x.loaders.iter().find(|x| x.id == loader_version_full));

                                if let Some(version) = version {
                                    return Ok::<Option<LoaderVersion>, Error>(Some(version.clone()));
                                }
                            }

                            info!("Forge - Installer Start {}", loader_version_full.clone());
                            let bytes = download_file(&format!("https://maven.minecraftforge.net/net/minecraftforge/forge/{0}/forge-{0}-installer.jar", loader_version_full), None, semaphore.clone()).await?;

                            // return Err(Error::TestError("test".to_owned()));

                            let reader = std::io::Cursor::new(bytes);

                            if let Ok(archive) = zip::ZipArchive::new(reader) {
                                if FORGE_MANIFEST_V1_QUERY.matches(&version) {
                                    let mut archive_clone = archive.clone();
                                    let profile = tokio::task::spawn_blocking(move || {
                                        let mut install_profile = archive_clone.by_name("install_profile.json")?;

                                        let mut contents = String::new();
                                        install_profile.read_to_string(&mut contents)?;

                                        Ok::<ForgeInstallerProfileV1, Error>(serde_json::from_str::<ForgeInstallerProfileV1>(&contents)?)
                                    }).await??;

                                    let mut archive_clone = archive.clone();
                                    let file_path = profile.install.file_path.clone();
                                    let forge_universal_bytes = tokio::task::spawn_blocking(move || {
                                        let mut forge_universal_file = archive_clone.by_name(&file_path)?;
                                        let mut forge_universal =  Vec::new();
                                        forge_universal_file.read_to_end(&mut forge_universal)?;


                                        Ok::<bytes::Bytes, Error>(bytes::Bytes::from(forge_universal))
                                    }).await??;
                                    let forge_universal_path = profile.install.path.clone();

                                    let now = Instant::now();
                                    let libs = futures::future::try_join_all(profile.version_info.libraries.into_iter().map(|mut lib| async {
                                        if let Some(url) = lib.url {
                                            {
                                                let mut visited_assets = visited_assets.lock().await;

                                                if visited_assets.contains(&lib.name) {
                                                    lib.url = Some(format_url("maven/"));

                                                    return Ok::<Library, Error>(lib);
                                                } else {
                                                    visited_assets.push(lib.name.clone())
                                                }
                                            }

                                            let artifact_path =
                                                daedalus::get_path_from_artifact(&lib.name)?;

                                            let artifact = if lib.name == forge_universal_path {
                                                forge_universal_bytes.clone()
                                            } else {
                                                let mirrors = vec![&*url, "https://maven.creeperhost.net/", "https://libraries.minecraft.net/"];

                                                download_file_mirrors(
                                                    &artifact_path,
                                                    &mirrors,
                                                    None,
                                                    semaphore.clone(),
                                                )
                                                    .await?
                                            };

                                            lib.url = Some(format_url("maven/"));

                                            upload_file_to_bucket(
                                                format!("{}/{}", "maven", artifact_path),
                                                artifact.to_vec(),
                                                Some("application/java-archive".to_string()),
                                                uploaded_files_mutex.as_ref(),
                                                semaphore.clone(),
                                            ).await?;
                                        }

                                        Ok::<Library, Error>(lib)
                                    })).await?;

                                    let elapsed = now.elapsed();
                                    info!("Elapsed lib DL: {:.2?}", elapsed);

                                    let new_profile = PartialVersionInfo {
                                        id: profile.version_info.id,
                                        inherits_from: profile.install.minecraft,
                                        release_time: profile.version_info.release_time,
                                        time: profile.version_info.time,
                                        main_class: profile.version_info.main_class,
                                        minecraft_arguments: profile.version_info.minecraft_arguments.clone(),
                                        arguments: profile.version_info.minecraft_arguments.map(|x| [(ArgumentType::Game, x.split(' ').map(|x| Argument::Normal(x.to_string())).collect())].iter().cloned().collect()),
                                        libraries: libs,
                                        type_: profile.version_info.type_,
                                        data: None,
                                        processors: None
                                    };

                                    let version_path = format!(
                                        "forge/v{}/versions/{}.json",
                                        daedalus::modded::CURRENT_FORGE_FORMAT_VERSION,
                                        new_profile.id
                                    );

                                    upload_file_to_bucket(
                                        version_path.clone(),
                                        serde_json::to_vec(&new_profile)?,
                                        Some("application/json".to_string()),
                                        uploaded_files_mutex.as_ref(),
                                        semaphore.clone(),
                                    ).await?;

                                    return Ok(Some(LoaderVersion {
                                        id: loader_version_full,
                                        url: format_url(&version_path),
                                        stable: false
                                    }));
                                } else if FORGE_MANIFEST_V2_QUERY_P1.matches(&version) || FORGE_MANIFEST_V2_QUERY_P2.matches(&version) || FORGE_MANIFEST_V3_QUERY.matches(&version) {
                                    let mut archive_clone = archive.clone();
                                    let mut profile = tokio::task::spawn_blocking(move || {
                                        let mut install_profile = archive_clone.by_name("install_profile.json")?;

                                        let mut contents = String::new();
                                        install_profile.read_to_string(&mut contents)?;

                                        Ok::<ForgeInstallerProfileV2, Error>(serde_json::from_str::<ForgeInstallerProfileV2>(&contents)?)
                                    }).await??;

                                    let mut archive_clone = archive.clone();
                                    let version_info = tokio::task::spawn_blocking(move || {
                                        let mut install_profile = archive_clone.by_name("version.json")?;

                                        let mut contents = String::new();
                                        install_profile.read_to_string(&mut contents)?;

                                        Ok::<PartialVersionInfo, Error>(serde_json::from_str::<PartialVersionInfo>(&contents)?)
                                    }).await??;


                                    let mut libs : Vec<Library> = version_info.libraries.into_iter().chain(profile.libraries.into_iter().map(|x| Library {
                                        downloads: x.downloads,
                                        extract: x.extract,
                                        name: x.name,
                                        url: x.url,
                                        natives: x.natives,
                                        rules: x.rules,
                                        checksums: x.checksums,
                                        include_in_classpath: false
                                    })).collect();

                                    let mut local_libs : HashMap<String, bytes::Bytes> = HashMap::new();

                                    for lib in &libs {
                                        if lib.downloads.as_ref().and_then(|x| x.artifact.as_ref().map(|x| x.url.is_empty())).unwrap_or(false) {
                                            let mut archive_clone = archive.clone();
                                            let lib_name_clone = lib.name.clone();

                                            let lib_bytes = tokio::task::spawn_blocking(move || {
                                                let mut lib_file = archive_clone.by_name(&format!("maven/{}", daedalus::get_path_from_artifact(&lib_name_clone)?))?;
                                                let mut lib_bytes =  Vec::new();
                                                lib_file.read_to_end(&mut lib_bytes)?;

                                                Ok::<bytes::Bytes, Error>(bytes::Bytes::from(lib_bytes))
                                            }).await??;

                                            local_libs.insert(lib.name.clone(), lib_bytes);
                                        }
                                    }

                                    let path = profile.path.clone();
                                    let version = profile.version.clone();

                                    for entry in profile.data.values_mut() {
                                        if entry.client.starts_with('/') || entry.server.starts_with('/') {
                                            macro_rules! read_data {
                                        ($value:expr) => {
                                            let mut archive_clone = archive.clone();
                                            let value_clone = $value.clone();
                                            let lib_bytes = tokio::task::spawn_blocking(move || {
                                                let mut lib_file = archive_clone.by_name(&value_clone[1..value_clone.len()])?;
                                                let mut lib_bytes =  Vec::new();
                                                lib_file.read_to_end(&mut lib_bytes)?;

                                                Ok::<bytes::Bytes, Error>(bytes::Bytes::from(lib_bytes))
                                            }).await??;

                                            let split = $value.split('/').last();

                                            if let Some(last) = split {
                                                let mut file = last.split('.');

                                                if let Some(file_name) = file.next() {
                                                    if let Some(ext) = file.next() {
                                                        let path = format!("{}:{}@{}", path.as_deref().unwrap_or(&*format!("net.minecraftforge:forge:{}", version)), file_name, ext);
                                                        $value = format!("[{}]", &path);
                                                        local_libs.insert(path.clone(), bytes::Bytes::from(lib_bytes));

                                                        libs.push(Library {
                                                            downloads: None,
                                                            extract: None,
                                                            name: path,
                                                            url: Some("".to_string()),
                                                            natives: None,
                                                            rules: None,
                                                            checksums: None,
                                                            include_in_classpath: false,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                    }

                                            if entry.client.starts_with('/') {
                                                read_data!(entry.client);
                                            }

                                            if entry.server.starts_with('/') {
                                                read_data!(entry.server);
                                            }
                                        }
                                    }

                                    let now = Instant::now();
                                    let libs = futures::future::try_join_all(libs.into_iter().map(|mut lib| async {
                                        let artifact_path =
                                            daedalus::get_path_from_artifact(&lib.name)?;

                                        {
                                            let mut visited_assets = visited_assets.lock().await;

                                            if visited_assets.contains(&lib.name) {
                                                if let Some(ref mut downloads) = lib.downloads {
                                                    if let Some(ref mut artifact) = downloads.artifact {
                                                        artifact.url = format_url(&format!("maven/{}", artifact_path));
                                                    }
                                                } else if lib.url.is_some() {
                                                    lib.url = Some(format_url("maven/"));
                                                }

                                                return Ok::<Library, Error>(lib);
                                            } else {
                                                visited_assets.push(lib.name.clone())
                                            }
                                        }

                                        let artifact_bytes = if let Some(ref mut downloads) = lib.downloads {
                                            if let Some(ref mut artifact) = downloads.artifact {
                                                let res = if artifact.url.is_empty() {
                                                    local_libs.get(&lib.name).cloned()
                                                } else {
                                                    Some(download_file(
                                                        &artifact.url,
                                                        Some(&*artifact.sha1),
                                                        semaphore.clone(),
                                                    )
                                                        .await?)
                                                };

                                                if res.is_some() {
                                                    artifact.url = format_url(&format!("maven/{}", artifact_path));
                                                }

                                                res
                                            } else { None }
                                        } else if let Some(ref mut url) = lib.url {
                                            let res = if url.is_empty() {
                                                local_libs.get(&lib.name).cloned()
                                            } else {
                                                Some(download_file(
                                                    url,
                                                    None,
                                                    semaphore.clone(),
                                                )
                                                    .await?)
                                            };

                                            if res.is_some() {
                                                lib.url = Some(format_url("maven/"));
                                            }

                                            res
                                        } else { None };

                                        if let Some(bytes) = artifact_bytes {
                                            upload_file_to_bucket(
                                                format!("{}/{}", "maven", artifact_path),
                                                bytes.to_vec(),
                                                Some("application/java-archive".to_string()),
                                                uploaded_files_mutex.as_ref(),
                                                semaphore.clone(),
                                            ).await?;
                                        }

                                        Ok::<Library, Error>(lib)
                                    })).await?;

                                    let elapsed = now.elapsed();
                                    info!("Elapsed lib DL: {:.2?}", elapsed);

                                    let new_profile = PartialVersionInfo {
                                        id: version_info.id,
                                        inherits_from: version_info.inherits_from,
                                        release_time: version_info.release_time,
                                        time: version_info.time,
                                        main_class: version_info.main_class,
                                        minecraft_arguments: version_info.minecraft_arguments,
                                        arguments: version_info.arguments,
                                        libraries: libs,
                                        type_: version_info.type_,
                                        data: Some(profile.data),
                                        processors: Some(profile.processors),
                                    };

                                    let version_path = format!(
                                        "forge/v{}/versions/{}.json",
                                        daedalus::modded::CURRENT_FORGE_FORMAT_VERSION,
                                        new_profile.id
                                    );

                                    upload_file_to_bucket(
                                        version_path.clone(),
                                        serde_json::to_vec(&new_profile)?,
                                        Some("application/json".to_string()),
                                        uploaded_files_mutex.as_ref(),
                                        semaphore.clone(),
                                    ).await?;

                                    return Ok(Some(LoaderVersion {
                                        id: loader_version_full,
                                        url: format_url(&version_path),
                                        stable: false
                                    }));
                                }
                            }

                            Ok(None)
                        }.await
                    });

                    {
                        let len = loaders_futures.len();
                        let mut versions = loaders_futures.into_iter().peekable();
                        let mut chunk_index = 0;
                        while versions.peek().is_some() {
                            let now = Instant::now();

                            let chunk: Vec<_> = versions.by_ref().take(1).collect();
                            // let res = futures::future::try_join_all(chunk).await?;
                            // loaders_versions.extend(res.into_iter().flatten());

                            // Flx DaedalusError FetchError TimedOut
                            match futures::future::try_join_all(chunk).await {
                                Ok(res) => loaders_versions.extend(res.into_iter().flatten()),
                                Err(err) => {
                                    error!("Loader Chunk Error: {:?}", err);
                                },
                            }

                            chunk_index += 1;

                            let elapsed = now.elapsed();
                            info!("Loader Chunk {}/{len} Elapsed: {:.2?}", chunk_index, elapsed);
                        }
                    }

                }

                versions.lock().await.push(daedalus::modded::Version {
                    id: minecraft_version,
                    stable: true,
                    loaders: loaders_versions
                });

                Ok::<(), Error>(())
            });
        }
    }

    {
        let len = version_futures.len();
        let mut versions = version_futures.into_iter().peekable();
        let mut chunk_index = 0;
        while versions.peek().is_some() {
            let now = Instant::now();

            let chunk: Vec<_> = versions.by_ref().take(1).collect();
            futures::future::try_join_all(chunk).await?;

            chunk_index += 1;

            let elapsed = now.elapsed();
            info!("Chunk {}/{len} Elapsed: {:.2?}", chunk_index, elapsed);
        }
    }

    if let Ok(versions) = Arc::try_unwrap(versions) {
        let mut versions = versions.into_inner();

        versions.sort_by(|x, y| {
            minecraft_versions
                .versions
=======
        fetch_versions
    } else {
        forge_versions.iter().collect()
    };

    if !fetch_versions.is_empty() {
        let forge_installers = futures::future::try_join_all(
            fetch_versions
>>>>>>> modrinth/master
                .iter()
                .map(|x| download_file(&x.installer_url, None, &semaphore)),
        )
        .await?;

        #[tracing::instrument(skip(raw, upload_files, mirror_artifacts))]
        async fn read_forge_installer(
            raw: bytes::Bytes,
            loader: &ForgeVersion,
            maven_url: &str,
            mod_loader: &str,
            upload_files: &DashMap<String, UploadFile>,
            mirror_artifacts: &DashMap<String, MirrorArtifact>,
        ) -> Result<PartialVersionInfo, Error> {
            tracing::trace!(
                "Reading forge installer for {}",
                loader.loader_version
            );
            type ZipFileReader = async_zip::base::read::seek::ZipFileReader<
                Cursor<bytes::Bytes>,
            >;

            let cursor = Cursor::new(raw);
            let mut zip = ZipFileReader::new(cursor).await?;

            #[tracing::instrument(skip(zip))]
            async fn read_file(
                zip: &mut ZipFileReader,
                file_name: &str,
            ) -> Result<Option<Vec<u8>>, Error> {
                let zip_index_option =
                    zip.file().entries().iter().position(|f| {
                        f.filename().as_str().unwrap_or_default() == file_name
                    });

                if let Some(zip_index) = zip_index_option {
                    let mut buffer = Vec::new();
                    let mut reader = zip.reader_with_entry(zip_index).await?;
                    reader.read_to_end_checked(&mut buffer).await?;

                    Ok(Some(buffer))
                } else {
                    Ok(None)
                }
            }

            #[tracing::instrument(skip(zip))]
            async fn read_json<T: DeserializeOwned>(
                zip: &mut ZipFileReader,
                file_name: &str,
            ) -> Result<Option<T>, Error> {
                if let Some(file) = read_file(zip, file_name).await? {
                    Ok(Some(serde_json::from_slice(&file)?))
                } else {
                    Ok(None)
                }
            }

            if loader.format_version == 1 {
                #[derive(Deserialize, Debug)]
                #[serde(rename_all = "camelCase")]
                struct ForgeInstallerProfileInstallDataV1 {
                    // pub mirror_list: String,
                    // pub target: String,
                    /// Path to the Forge universal library
                    pub file_path: String,
                    // pub logo: String,
                    // pub welcome: String,
                    // pub version: String,
                    /// Maven coordinates of the Forge universal library
                    pub path: String,
                    // pub profile_name: String,
                    pub minecraft: String,
                }

                #[derive(Deserialize, Debug)]
                #[serde(rename_all = "camelCase")]
                struct ForgeInstallerProfileManifestV1 {
                    pub id: String,
                    pub libraries: Vec<daedalus::minecraft::Library>,
                    pub main_class: Option<String>,
                    pub minecraft_arguments: Option<String>,
                    pub release_time: DateTime<Utc>,
                    pub time: DateTime<Utc>,
                    pub type_: daedalus::minecraft::VersionType,
                    // pub assets: Option<String>,
                    // pub inherits_from: Option<String>,
                    // pub jar: Option<String>,
                }

                #[derive(Deserialize, Debug)]
                #[serde(rename_all = "camelCase")]
                struct ForgeInstallerProfileV1 {
                    pub install: ForgeInstallerProfileInstallDataV1,
                    pub version_info: ForgeInstallerProfileManifestV1,
                }

                let install_profile = read_json::<ForgeInstallerProfileV1>(
                    &mut zip,
                    "install_profile.json",
                )
                .await?
                .ok_or_else(|| {
                    crate::ErrorKind::InvalidInput(format!(
                        "No install_profile.json present for loader {}",
                        loader.installer_url
                    ))
                })?;

                let forge_library =
                    read_file(&mut zip, &install_profile.install.file_path)
                        .await?
                        .ok_or_else(|| {
                            crate::ErrorKind::InvalidInput(format!(
                                "No forge library present for loader {}",
                                loader.installer_url
                            ))
                        })?;

                upload_files.insert(
                    format!(
                        "maven/{}",
                        get_path_from_artifact(&install_profile.install.path)?
                    ),
                    UploadFile {
                        file: bytes::Bytes::from(forge_library),
                        content_type: None,
                    },
                );

                Ok(PartialVersionInfo {
                    id: install_profile.version_info.id,
                    inherits_from: install_profile.install.minecraft,
                    release_time: install_profile.version_info.release_time,
                    time: install_profile.version_info.time,
                    main_class: install_profile.version_info.main_class,
                    minecraft_arguments: install_profile
                        .version_info
                        .minecraft_arguments
                        .clone(),
                    arguments: install_profile
                        .version_info
                        .minecraft_arguments
                        .map(|x| {
                            [(
                                daedalus::minecraft::ArgumentType::Game,
                                x.split(' ')
                                    .map(|x| {
                                        daedalus::minecraft::Argument::Normal(
                                            x.to_string(),
                                        )
                                    })
                                    .collect(),
                            )]
                            .iter()
                            .cloned()
                            .collect()
                        }),
                    libraries: install_profile
                        .version_info
                        .libraries
                        .into_iter()
                        .map(|mut lib| {
                            // For all libraries besides the forge lib extracted, we mirror them from maven servers
                            // unless the URL is empty/null or available on Minecraft's servers
                            if let Some(url) = lib.url {
                                if lib.name != install_profile.install.path
                                    && !url.is_empty()
                                    && !url.contains(
                                        "https://libraries.minecraft.net/",
                                    )
                                {
                                    insert_mirrored_artifact(
                                        &lib.name,
                                        None,
                                        vec![
                                            url,
                                            "https://maven.creeperhost.net/"
                                                .to_string(),
                                            maven_url.to_string(),
                                        ],
                                        false,
                                        mirror_artifacts,
                                    )?;
                                }
                            }

                            lib.url = Some(format_url("maven/"));

                            Ok(lib)
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                    type_: install_profile.version_info.type_,
                    data: None,
                    processors: None,
                })
            } else if loader.format_version == 2 {
                #[derive(Deserialize, Debug)]
                #[serde(rename_all = "camelCase")]
                struct ForgeInstallerProfileV2 {
                    // pub spec: i32,
                    // pub profile: String,
                    // pub version: String,
                    // pub json: String,
                    pub path: Option<String>,
                    // pub minecraft: String,
                    pub data: HashMap<String, daedalus::modded::SidedDataEntry>,
                    pub libraries: Vec<daedalus::minecraft::Library>,
                    pub processors: Vec<daedalus::modded::Processor>,
                }

                let install_profile = read_json::<ForgeInstallerProfileV2>(
                    &mut zip,
                    "install_profile.json",
                )
                .await?
                .ok_or_else(|| {
                    crate::ErrorKind::InvalidInput(format!(
                        "No install_profile.json present for loader {}",
                        loader.installer_url
                    ))
                })?;

                let mut version_info =
                    read_json::<PartialVersionInfo>(&mut zip, "version.json")
                        .await?
                        .ok_or_else(|| {
                            crate::ErrorKind::InvalidInput(format!(
                                "No version.json present for loader {}",
                                loader.installer_url
                            ))
                        })?;

                version_info.processors = Some(install_profile.processors);
                version_info.libraries.extend(
                    install_profile.libraries.into_iter().map(|mut x| {
                        x.include_in_classpath = false;

                        x
                    }),
                );

                async fn mirror_forge_library(
                    mut zip: ZipFileReader,
                    mut lib: daedalus::minecraft::Library,
                    maven_url: &str,
                    upload_files: &DashMap<String, UploadFile>,
                    mirror_artifacts: &DashMap<String, MirrorArtifact>,
                ) -> Result<daedalus::minecraft::Library, Error>
                {
                    let artifact_path = get_path_from_artifact(&lib.name)?;

                    if let Some(ref mut artifact) =
                        lib.downloads.as_mut().and_then(|x| x.artifact.as_mut())
                    {
                        if !artifact.url.is_empty() {
                            insert_mirrored_artifact(
                                &lib.name,
                                Some(artifact.sha1.clone()),
                                vec![artifact.url.clone()],
                                true,
                                mirror_artifacts,
                            )?;

                            artifact.url =
                                format_url(&format!("maven/{}", artifact_path));

                            return Ok(lib);
                        }
                    } else if let Some(url) = &lib.url {
                        if !url.is_empty() {
                            insert_mirrored_artifact(
                                &lib.name,
                                None,
                                vec![
                                    url.clone(),
                                    "https://libraries.minecraft.net/"
                                        .to_string(),
                                    "https://maven.creeperhost.net/"
                                        .to_string(),
                                    maven_url.to_string(),
                                ],
                                false,
                                mirror_artifacts,
                            )?;

                            lib.url = Some(format_url("maven/"));

                            return Ok(lib);
                        }
                    }

                    // Other libraries are generally available in the "maven" directory of the installer. If they are
                    // not present here, they will be generated by Forge processors.
                    let extract_path = format!("maven/{artifact_path}");
                    if let Some(file) =
                        read_file(&mut zip, &extract_path).await?
                    {
                        upload_files.insert(
                            extract_path,
                            UploadFile {
                                file: bytes::Bytes::from(file),
                                content_type: None,
                            },
                        );

                        lib.url = Some(format_url("maven/"));
                    } else {
                        lib.downloadable = false;
                    }

                    Ok(lib)
                }

                version_info.libraries = futures::future::try_join_all(
                    version_info.libraries.into_iter().map(|lib| {
                        mirror_forge_library(
                            zip.clone(),
                            lib,
                            maven_url,
                            upload_files,
                            mirror_artifacts,
                        )
                    }),
                )
                .await?;

                // In Minecraft Forge modern installers, processors are run during the install process. Some processors
                // are extracted from the installer JAR. This function finds these files, extracts them, and uploads them
                // and registers them as libraries instead.
                // Ex:
                // "BINPATCH": {
                //      "client": "/data/client.lzma",
                //      "server": "/data/server.lzma"
                //     },
                // Becomes:
                // "BINPATCH": {
                //      "client": "[net.minecraftforge:forge:1.20.3-49.0.1:shim:client@lzma]",
                //      "server": "[net.minecraftforge:forge:1.20.3-49.0.1:shim:server@lzma]"
                // },
                // And the resulting library is added to the profile's libraries
                let mut new_data = HashMap::new();
                for (key, entry) in install_profile.data {
                    async fn extract_data(
                        zip: &mut ZipFileReader,
                        key: &str,
                        value: &str,
                        upload_files: &DashMap<String, UploadFile>,
                        libs: &mut Vec<daedalus::minecraft::Library>,
                        mod_loader: &str,
                        version: &ForgeVersion,
                    ) -> Result<String, Error> {
                        let extract_file =
                            read_file(zip, &value[1..value.len()])
                                .await?
                                .ok_or_else(|| {
                                    crate::ErrorKind::InvalidInput(format!(
                                "Unable reading data key {key} at path {value}",
                            ))
                                })?;

                        let file_name = value.split('/').last()
                            .ok_or_else(|| {
                                crate::ErrorKind::InvalidInput(format!(
                                    "Unable reading filename for data key {key} at path {value}",

                                ))
                            })?;

                        let mut file = file_name.split('.');
                        let file_name = file.next()
                            .ok_or_else(|| {
                                crate::ErrorKind::InvalidInput(format!(
                                    "Unable reading filename only for data key {key} at path {value}",
                                ))
                            })?;
                        let ext = file.next()
                            .ok_or_else(|| {
                                crate::ErrorKind::InvalidInput(format!(
                                    "Unable reading extension only for data key {key} at path {value}",
                                ))
                            })?;

                        let path = format!(
                            "com.modrinth.daedalus:{}-installer-extracts:{}:{}@{}",
                            mod_loader,
                            version.raw,
                            file_name,
                            ext
                        );

                        upload_files.insert(
                            format!("maven/{}", get_path_from_artifact(&path)?),
                            UploadFile {
                                file: bytes::Bytes::from(extract_file),
                                content_type: None,
                            },
                        );

                        libs.push(daedalus::minecraft::Library {
                            downloads: None,
                            extract: None,
                            name: path.clone(),
                            url: Some(format_url("maven/")),
                            natives: None,
                            rules: None,
                            checksums: None,
                            include_in_classpath: false,
                            downloadable: true,
                        });

                        Ok(format!("[{path}]"))
                    }

                    let client = if entry.client.starts_with('/') {
                        extract_data(
                            &mut zip,
                            &key,
                            &entry.client,
                            upload_files,
                            &mut version_info.libraries,
                            mod_loader,
                            loader,
                        )
                        .await?
                    } else {
                        entry.client.clone()
                    };

                    let server = if entry.server.starts_with('/') {
                        extract_data(
                            &mut zip,
                            &key,
                            &entry.server,
                            upload_files,
                            &mut version_info.libraries,
                            mod_loader,
                            loader,
                        )
                        .await?
                    } else {
                        entry.server.clone()
                    };

                    new_data.insert(
                        key.clone(),
                        daedalus::modded::SidedDataEntry { client, server },
                    );
                }

                version_info.data = Some(new_data);

                Ok(version_info)
            } else {
                Err(crate::ErrorKind::InvalidInput(format!(
                    "Unknown format version {} for loader {}",
                    loader.format_version, loader.installer_url
                ))
                .into())
            }
        }

        let forge_version_infos = futures::future::try_join_all(
            forge_installers
                .into_iter()
                .enumerate()
                .map(|(index, raw)| {
                    let loader = fetch_versions[index];

                    read_forge_installer(
                        raw,
                        loader,
                        maven_url,
                        mod_loader,
                        upload_files,
                        mirror_artifacts,
                    )
                }),
        )
        .await?;

        let serialized_version_manifests = forge_version_infos
            .iter()
            .map(|x| serde_json::to_vec(x).map(bytes::Bytes::from))
            .collect::<Result<Vec<_>, serde_json::Error>>()?;

        serialized_version_manifests
            .into_iter()
            .enumerate()
            .for_each(|(index, bytes)| {
                let loader = fetch_versions[index];

                let version_path = format!(
                    "{mod_loader}/v{format_version}/versions/{}.json",
                    loader.loader_version
                );

                upload_files.insert(
                    version_path,
                    UploadFile {
                        file: bytes,
                        content_type: Some("application/json".to_string()),
                    },
                );
            });

        let forge_manifest_path =
            format!("{mod_loader}/v{format_version}/manifest.json",);

        let manifest = daedalus::modded::Manifest {
            game_versions: forge_versions
                .into_iter()
                .rev()
                .chunk_by(|x| x.game_version.clone())
                .into_iter()
                .map(|(game_version, loaders)| daedalus::modded::Version {
                    id: game_version,
                    stable: true,
                    loaders: loaders
                        .map(|x| daedalus::modded::LoaderVersion {
                            url: format_url(&format!(
                        "{mod_loader}/v{format_version}/versions/{}.json",
                        x.loader_version
                    )),
                            id: x.loader_version,
                            stable: false,
                        })
                        .collect(),
                })
                .collect(),
        };

        upload_files.insert(
            forge_manifest_path,
            UploadFile {
                file: bytes::Bytes::from(serde_json::to_vec(&manifest)?),
                content_type: Some("application/json".to_string()),
            },
        );
    }

    Ok(())
}

#[derive(Debug)]
struct ForgeVersion {
    pub format_version: usize,
    pub raw: String,
    pub loader_version: String,
    pub game_version: String,
    pub installer_url: String,
}
