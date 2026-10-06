// Package trader owns the bot's state, persistence, market feeds, exchange
// adapter, and execution lifecycle. Keeping these implementations private to
// one package makes the pair gates and durable transaction boundaries explicit.
package trader
