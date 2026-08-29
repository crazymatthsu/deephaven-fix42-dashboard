# Deephaven Application Mode descriptor -- the "copy me" template.
#
# Run it with:   DH_APP=example-minimal podman compose -f docker/docker-compose.yml up -d
#
# Deliberately the smallest thing that is still a real app: it depends on no
# repo code, so bringing it up proves the DH_APP switch by itself. To start a
# real service, copy this folder, rename the .app file and the id, and point
# main.py at your entrypoint under deephaven-scripts/src/ the way
# apps/fix42-dashboard/main.py does.

type=script
scriptType=python
enabled=true
id=example.minimal
name=Example Minimal App
file_0=main.py
