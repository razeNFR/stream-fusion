from cachetools import TTLCache
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.settings import settings
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.security.security_api_key import check_api_key
from stream_fusion.version import get_version
from stream_fusion.web.root.config.schemas import ManifestResponse

router = APIRouter()

templates = Jinja2Templates(directory="/app/stream_fusion/static")
stream_cache = TTLCache(maxsize=1000, ttl=3600)


@router.get("/")
async def root():
    logger.info("Redirecting to /configure")
    return RedirectResponse(url="/configure")


@router.get("/configure")
@router.get("/{config}/configure")
async def configure(request: Request):
    logger.info("Serving configuration page")
    return templates.TemplateResponse("index.html", {
        "request": request,
        "rd_unique_account": settings.rd_unique_account,
        "ad_unique_account": settings.ad_unique_account,
        "sharewood_unique_account": settings.sharewood_unique_account,
        "ygg_unique_account": settings.ygg_unique_account,
        "jackett_enable": settings.jackett_enable,
        "tb_unique_account": settings.tb_unique_account,
    })


# @router.get("/static/{file_path:path}", response_model=StaticFileResponse)
# async def serve_static(file_path: str):
#     logger.debug(f"Serving static file: {file_path}")
#     return FileResponse(f"/app/stream_fusion/static/{file_path}")


@router.get("/manifest.json")
async def get_manifest():
    logger.info("Serving manifest.json")
    return ManifestResponse(
        id="community.limedrive.streamfusion",
        icon="https://i.imgur.com/q2VSdSp.png",
        version=str(get_version()),
        resources=[
            'catalog',
            {
                'name': 'stream', 
                'types': ['movie', 'series'], 
                'idPrefixes': ['tt']
            }
        ],
        types=["movie", "series"],
        name="razeNio" + (" (dev)" if settings.develop else ""),
        description="StreamFusion enhances Stremio by integrating torrent indexers and debrid services, "
                    "providing access to a vast array of cached torrent sources. This plugin seamlessly bridges "
                    "Stremio with popular indexers and debrid platforms, offering users an expanded content "
                    "library and a smooth streaming experience.",
        catalogs=[
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "Yggflix - Films Récents"
            },
            {
                "type": "movie",
                "id": "recently_added_movies",
                "name": "YGGtorrent - Films Récemment Ajoutés"
            },
            {
                "type": "series",
                "id": "latest_tv_shows",
                "name": "Yggflix - Séries Récentes"
            },
            {
                "type": "series",
                "id": "recently_added_tv_shows",
                "name": "YGGtorrent - Séries Récemment Ajoutées"
            }
        ],
        behaviorHints={
            "configurable": True,
            "configurationRequired": True
        },
        config=[
            {
                "key": "api_key",
                "title": "API Key",
                "type": "text",
                "required": True
            }
        ]
    )

@router.get("/{config}/manifest.json")
async def get_manifest(config: str, apikey_dao: APIKeyDAO = Depends()):
    config = parse_config(config)
    api_key = config.get("apiKey")
    if api_key:
        await check_api_key(api_key, apikey_dao)
    else:
        # Check if anonymous access is allowed
        if not settings.allow_anonymous_access: # If NOT allowed
            logger.warning("Anonymous access denied and API key not found in config.")
            raise HTTPException(status_code=401, detail="API key required or anonymous access disabled.")
        else: # If anonymous access IS allowed, just log and continue
            logger.info("Proceeding without API key (anonymous access allowed).")
            # No exception is raised, execution continues

    yggflix_ctg = config.get("yggflixCtg", True)
    yggtorrent_ctg = config.get("yggtorrentCtg", True)

    catalogs = []

    if yggflix_ctg:
        catalogs.extend([
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "Yggflix"
            },
            {
                "type": "series",
                "id": "latest_tv_shows",
                "name": "Yggflix"
            }
        ])

    if yggtorrent_ctg:
        catalogs.extend([
            {
                "type": "movie",
                "id": "recently_added_movies",
                "name": "YGGtorrent - Récemment Ajoutés"
            },
            {
                "type": "series",
                "id": "recently_added_tv_shows",
                "name": "YGGtorrent - Récemment Ajoutées"
            }
        ])

    logger.info("Serving manifest.json")
    return ManifestResponse(
        id="community.limedrive.streamfusion",
        icon="https://i.imgur.com/q2VSdSp.png",
        version=str(get_version()),
        resources=[
            'catalog',
            {
                'name': 'stream', 
                'types': ['movie', 'series'], 
                'idPrefixes': ['tt']
            }
        ],
        types=["movie", "series"],
        name="razeNio" + (" (dev)" if settings.develop else ""),
        description="StreamFusion enhances Stremio by integrating torrent indexers and debrid services,"
         " providing access to a vast array of cached torrent sources. This plugin seamlessly bridges"
         " Stremio with popular indexers and debrid platforms, offering users an expanded content"
         " library and a smooth streaming experience.",
        catalogs=catalogs,
    )
