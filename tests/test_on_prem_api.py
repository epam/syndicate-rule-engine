from onprem.api.app import OnPremApiBuilder, AuthPlugin


def test_to_bottle_route():
    assert OnPremApiBuilder.to_bottle_route('/path/{id}') == '/path/<id>'
    assert OnPremApiBuilder.to_bottle_route('/{test}') == '/<test>'
    assert OnPremApiBuilder.to_bottle_route(
        '/path/{one}/{two}/') == '/path/<one>/<two>/'
    assert OnPremApiBuilder.to_bottle_route('/path/') == '/path/'
    assert OnPremApiBuilder.to_bottle_route('/path') == '/path'
    assert OnPremApiBuilder.to_bottle_route('/path/<one>') == '/path/<one>'


def test_get_token_from_header():
    assert AuthPlugin.get_token_from_header('qwerty') == 'qwerty'
    assert AuthPlugin.get_token_from_header('') is None
    assert AuthPlugin.get_token_from_header('Bearer qwerty') == 'qwerty'
    assert AuthPlugin.get_token_from_header('bearer qwerty') == 'qwerty'
    assert AuthPlugin.get_token_from_header('bearer  qwerty') == 'qwerty'
