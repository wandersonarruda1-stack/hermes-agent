"""Register the file-backed hosted-room service provider."""
from hermes_cli.dashboard_auth.service import ProfileServiceProvider, TICKET_ROUTE
from hermes_cli.dashboard_auth.token_auth import register_token_route


def register(ctx):
    ctx.register_dashboard_auth_provider(ProfileServiceProvider())
    register_token_route(TICKET_ROUTE)
