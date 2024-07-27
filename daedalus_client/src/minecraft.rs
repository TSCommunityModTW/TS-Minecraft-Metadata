use crate::util::fetch_json;
use crate::{
    util::download_file, util::format_url, util::sha1_async, Error,
    MirrorArtifact, UploadFile,
};
<<<<<<< HEAD
use log::{error, info};
=======
use daedalus::minecraft::{
    merge_partial_library, Library, PartialLibrary, VersionInfo,
    VersionManifest, VERSION_MANIFEST_URL,
};
use dashmap::DashMap;
>>>>>>> modrinth/master
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[tracing::instrument(skip(semaphore, upload_files, _mirror_artifacts))]
pub async fn fetch(
    semaphore: Arc<Semaphore>,
    upload_files: &DashMap<String, UploadFile>,
    _mirror_artifacts: &DashMap<String, MirrorArtifact>,
) -> Result<(), Error> {
    let modrinth_manifest = fetch_json::<VersionManifest>(
        &format_url(&format!(
            "minecraft/v{}/manifest.json",
            daedalus::minecraft::CURRENT_FORMAT_VERSION
        )),
        &semaphore,
    )
    .await
    .ok();
    let mojang_manifest =
        fetch_json::<VersionManifest>(VERSION_MANIFEST_URL, &semaphore).await?;

    // TODO: experimental snapshots: https://github.com/PrismLauncher/meta/blob/main/meta/common/mojang-minecraft-experiments.json
    // TODO: old snapshots: https://github.com/PrismLauncher/meta/blob/main/meta/common/mojang-minecraft-old-snapshots.json

    // We check Modrinth's version manifest and compare if the version 1) exists in Modrinth's database and 2) is unchanged
    // If they are not, we will fetch them
    let (fetch_versions, existing_versions) =
        if let Some(mut modrinth_manifest) = modrinth_manifest {
            let (mut fetch_versions, mut existing_versions) =
                (Vec::new(), Vec::new());

            for version in mojang_manifest.versions {
                if let Some(index) = modrinth_manifest
                    .versions
                    .iter()
                    .position(|x| x.id == version.id)
                {
                    let modrinth_version =
                        modrinth_manifest.versions.remove(index);

                    if modrinth_version
                        .original_sha1
                        .as_ref()
                        .map(|x| x == &version.sha1)
                        .unwrap_or(false)
                    {
                        existing_versions.push(modrinth_version);
                    } else {
                        fetch_versions.push(version);
                    }
                } else {
                    fetch_versions.push(version);
                }
            }

            (fetch_versions, existing_versions)
        } else {
            (mojang_manifest.versions, Vec::new())
        };

    if !fetch_versions.is_empty() {
        let version_manifests = futures::future::try_join_all(
            fetch_versions
                .iter()
                .map(|x| download_file(&x.url, Some(&x.sha1), &semaphore)),
        )
        .await?
        .into_iter()
        .map(|x| serde_json::from_slice(&x))
        .collect::<Result<Vec<VersionInfo>, serde_json::Error>>()?;

        // Patch libraries of Minecraft versions for M-series Mac Support, Better Linux Compatibility, etc
        let library_patches = fetch_library_patches()?;
        let patched_version_manifests = version_manifests
            .into_iter()
            .map(|mut x| {
                if !library_patches.is_empty() {
                    let mut new_libraries = Vec::new();
                    for library in x.libraries {
                        let mut libs = patch_library(&library_patches, library);
                        new_libraries.append(&mut libs)
                    }
                    x.libraries = new_libraries
                }

                x
            })
            .collect::<Vec<_>>();

        // serialize + compute hashes
        let serialized_version_manifests = patched_version_manifests
            .iter()
            .map(|x| serde_json::to_vec(x).map(bytes::Bytes::from))
            .collect::<Result<Vec<_>, serde_json::Error>>()?;
        let hashes_version_manifests = futures::future::try_join_all(
            serialized_version_manifests
                .iter()
                .map(|x| sha1_async(x.clone())),
        )
        .await?;

        // We upload the new version manifests and add them to the versions list
        let mut new_versions = patched_version_manifests
            .into_iter()
            .zip(serialized_version_manifests.into_iter())
            .zip(hashes_version_manifests.into_iter())
            .map(|((version, bytes), hash)| {
                let version_path = format!(
                    "minecraft/v{}/versions/{}.json",
                    daedalus::minecraft::CURRENT_FORMAT_VERSION,
                    version.id
                );

                let url = format_url(&version_path);
                upload_files.insert(
                    version_path,
                    UploadFile {
                        file: bytes,
                        content_type: Some("application/json".to_string()),
                    },
                );

                daedalus::minecraft::Version {
                    original_sha1: fetch_versions
                        .iter()
                        .find(|x| x.id == version.id)
                        .map(|x| x.sha1.clone()),
                    id: version.id,
                    type_: version.type_,
                    url,
                    time: version.time,
                    release_time: version.release_time,
                    sha1: hash,
                    compliance_level: 1,
                }
            })
            .chain(existing_versions.into_iter())
            .collect::<Vec<_>>();

        new_versions.sort_by(|a, b| b.release_time.cmp(&a.release_time));

<<<<<<< HEAD
                {
                    let mut visited_assets = visited_assets_mutex.lock().await;

                    if !visited_assets.contains(&version_info.asset_index.id) {
                        if let Some(assets_hash) = assets_hash {
                            if version_info.asset_index.sha1 != assets_hash {
                                download_assets = true;
                            }
                        } else {
                            download_assets = true;
                        }
                    }

                    if download_assets {
                        visited_assets
                            .push(version_info.asset_index.id.clone());
                    }
                }

                if download_assets {
                    let assets_index = download_file(
                        &assets_index_url,
                        Some(&version_info.asset_index.sha1),
                        semaphore.clone(),
                    )
                    .await?;

                    {
                        upload_futures.push(upload_file_to_bucket(
                            assets_path,
                            assets_index.to_vec(),
                            Some("application/json".to_string()),
                            uploaded_files_mutex.as_ref(),
                            semaphore.clone(),
                        ));
                    }
                }

                {
                    upload_futures.push(upload_file_to_bucket(
                        version_path,
                        serde_json::to_vec(&version_info)?,
                        Some("application/json".to_string()),
                        uploaded_files_mutex.as_ref(),
                        semaphore.clone(),
                    ));
                }

                futures::future::try_join_all(upload_futures).await?;

                Ok::<(), Error>(())
            }
            .await?;

            Ok::<(), Error>(())
        })
    }

    {
        let mut versions = version_futures.into_iter().peekable();
        let mut chunk_index = 0;
        while versions.peek().is_some() {
            let now = Instant::now();

            let chunk: Vec<_> = versions.by_ref().take(100).collect();
            // futures::future::try_join_all(chunk).await?;

            // Flx DaedalusError FetchError TimedOut
            match futures::future::try_join_all(chunk).await {
                Ok(res) => {}
                Err(err) => {
                    error!("Minecraft Chunk Error: {:?}", err);
                }
            }

            chunk_index += 1;

            let elapsed = now.elapsed();
            info!("Chunk {} Elapsed: {:.2?}", chunk_index, elapsed);
        }
    }
    //futures::future::try_join_all(version_futures).await?;

    upload_file_to_bucket(
        format!(
=======
        // create and upload the new manifest
        let version_manifest_path = format!(
>>>>>>> modrinth/master
            "minecraft/v{}/manifest.json",
            daedalus::minecraft::CURRENT_FORMAT_VERSION
        );

        let new_manifest = VersionManifest {
            latest: mojang_manifest.latest,
            versions: new_versions,
        };

        upload_files.insert(
            version_manifest_path,
            UploadFile {
                file: bytes::Bytes::from(serde_json::to_vec(&new_manifest)?),
                content_type: Some("application/json".to_string()),
            },
        );
    }

    Ok(())
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LibraryPatch {
    #[serde(rename = "_comment")]
    pub _comment: String,
    #[serde(rename = "match")]
    pub match_: Vec<String>,
    pub additional_libraries: Option<Vec<Library>>,
    #[serde(rename = "override")]
    pub override_: Option<PartialLibrary>,
    pub patch_additional_libraries: Option<bool>,
}

fn fetch_library_patches() -> Result<Vec<LibraryPatch>, Error> {
    let patches = include_bytes!("../library-patches.json");
    Ok(serde_json::from_slice(patches)?)
}

pub fn patch_library(
    patches: &Vec<LibraryPatch>,
    mut library: Library,
) -> Vec<Library> {
    let mut val = Vec::new();

    let actual_patches = patches
        .iter()
        .filter(|x| x.match_.contains(&library.name))
        .collect::<Vec<_>>();

    if !actual_patches.is_empty() {
        for patch in actual_patches {
            if let Some(override_) = &patch.override_ {
                library = merge_partial_library(override_.clone(), library);
            }

            if let Some(additional_libraries) = &patch.additional_libraries {
                for additional_library in additional_libraries {
                    if patch.patch_additional_libraries.unwrap_or(false) {
                        let mut libs =
                            patch_library(patches, additional_library.clone());
                        val.append(&mut libs)
                    } else {
                        val.push(additional_library.clone());
                    }
                }
            }
        }

        val.push(library);
    } else {
        val.push(library);
    }

    val
}
