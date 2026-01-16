RELEASE CHECKLIST
=================

This checklist documents the manual steps for creating a release and publishing the `foip` image.

- Bump version / tag
  - Create lightweight git tag locally: `git tag foip-p.v1`
  - Push tag: `git push origin foip-p.v1`

- Build & publish image (CI preferred)
  - In CI: build image, tag immutably (e.g. `foip:prod-<sha>`), sign with `cosign`, and push to registry.
  - If building locally (only for emergency):
    ```bash
    docker compose build fiop-platform
    docker tag foip:latest REGISTRY/OWNER/foip:p.v1
    docker push REGISTRY/OWNER/foip:p.v1
    ```

- Backup current running container (optional emergency backup)
  - Commit running container to image: `docker commit <container> foip:p.v1`
  - Save to tar: `docker save -o ./backups/fiop-p.v1.tar foip:p.v1`

- Create GitHub Release (recommended)
  - Create a Release using the `foip-p.v1` tag, add changelog and links to backup/artifacts.

- Update deployment manifests
  - Ensure `docker-compose.yaml` or k8s manifests reference the registry image tag.
  - Ensure Prometheus `bearer_token_file` or secret mount is in place for metrics scraping.

- Post-deploy smoke tests
  - Hit readiness: `curl -H "Authorization: Bearer $(cat ./secrets/health_bearer_token)" http://127.0.0.1:18090/ready`
  - Check metrics access with metrics token.

- Rollback plan
  - Known-good image: `docker pull REGISTRY/OWNER/foip:<previous-tag>`
  - Start with previous image: `docker compose up -d --no-build fiop-platform`

- Notifications & records
  - Announce release to team channel and update `RUNBOOK.md` if needed.

Keep backups in durable artifact storage (S3/GCS) and do NOT commit backup tarballs to git.
