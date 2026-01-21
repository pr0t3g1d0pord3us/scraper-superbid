#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - SUPERBID (SCHEMA REAL) + HEARTBEAT
✅ Mapeamento completo para schema existente
✅ ~100 campos + JSONBs (auction_address, seller_phone, seller_company)
✅ Compatível com triggers: extract_city_state_superbid, update_updated_at_column
✅ Sistema de heartbeat integrado (infra_actions)
"""

import os
import time
import requests
import traceback
from datetime import datetime
from typing import List, Dict, Optional, Any


class SupabaseSuperbid:
    """Cliente Supabase para schema real superbid_items com heartbeat integrado"""
    
    def __init__(self, service_name: str = 'superbid_scraper'):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        self.table = 'superbid_items'
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
            'Prefer': 'resolution=merge-duplicates,return=minimal'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # ============================================
        # HEARTBEAT - Configuração
        # ============================================
        self.service_name = service_name
        self.heartbeat_enabled = True
        self.heartbeat_start_time = time.time()
        self.heartbeat_metrics = {
            'items_processed': 0,
            'categories_scraped': 0,
            'errors': 0,
            'warnings': 0,
        }
    
    # ============================================
    # MÉTODOS HEARTBEAT
    # ============================================
    
    def _send_heartbeat(self, status: str, logs: Optional[Dict] = None, 
                        error_message: Optional[str] = None, 
                        metadata: Optional[Dict] = None) -> bool:
        """Envia heartbeat para infra_actions"""
        if not self.heartbeat_enabled:
            return False
        
        try:
            elapsed = time.time() - self.heartbeat_start_time
            
            full_logs = {
                'timestamp': datetime.now().isoformat(),
                'elapsed_seconds': round(elapsed, 2),
                'metrics': self.heartbeat_metrics.copy(),
                **(logs or {})
            }
            
            payload = {
                'service_name': self.service_name,
                'service_type': 'scraper',
                'status': status,
                'last_activity': datetime.now().isoformat(),
                'logs': full_logs,
                'error_message': error_message,
                'metadata': metadata or {}
            }
            
            # ✅ FIX: Headers EXPLÍCITOS para schema PUBLIC (infra_actions)
            heartbeat_headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Content-Profile': 'public',  # ✅ Schema público
                'Accept-Profile': 'public',    # ✅ Schema público
                'Prefer': 'resolution=merge-duplicates,return=minimal'
            }
            
            url = f"{self.url}/rest/v1/infra_actions?on_conflict=service_name"
            r = self.session.post(url, json=[payload], headers=heartbeat_headers, timeout=30)
            
            return r.status_code in (200, 201)
                
        except Exception as e:
            print(f"⚠️ Erro ao enviar heartbeat: {e}")
            return False
    
    def heartbeat_start(self, custom_logs: Optional[Dict] = None) -> bool:
        """Registra início da execução do scraper"""
        logs = {
            'event': 'start',
            'message': 'Scraper iniciado',
            **(custom_logs or {})
        }
        result = self._send_heartbeat(status='active', logs=logs)
        if result:
            print("💓 Heartbeat: Início registrado")
        return result
    
    def heartbeat_progress(self, items_processed: int = 0, categories_scraped: int = 0,
                          custom_logs: Optional[Dict] = None) -> bool:
        """Atualiza progresso durante execução"""
        self.heartbeat_metrics['items_processed'] += items_processed
        self.heartbeat_metrics['categories_scraped'] += categories_scraped
        
        logs = {
            'event': 'progress',
            'message': f"Processados {self.heartbeat_metrics['items_processed']} itens",
            **(custom_logs or {})
        }
        
        return self._send_heartbeat(status='active', logs=logs)
    
    def heartbeat_success(self, final_stats: Optional[Dict] = None) -> bool:
        """Registra conclusão com sucesso"""
        logs = {
            'event': 'completed',
            'message': 'Scraper concluído com sucesso',
            'final_stats': final_stats or {},
        }
        result = self._send_heartbeat(status='active', logs=logs)
        if result:
            print("💓 Heartbeat: Sucesso registrado")
        return result
    
    def heartbeat_error(self, error: Exception, context: Optional[str] = None) -> bool:
        """Registra erro durante execução"""
        self.heartbeat_metrics['errors'] += 1
        
        error_message = f"{type(error).__name__}: {str(error)}"
        if context:
            error_message = f"[{context}] {error_message}"
        
        logs = {
            'event': 'error',
            'error_type': type(error).__name__,
            'traceback': traceback.format_exc(),
            'context': context
        }
        
        result = self._send_heartbeat(
            status='error',
            logs=logs,
            error_message=error_message
        )
        if result:
            print("💓 Heartbeat: Erro registrado")
        return result
    
    def heartbeat_warning(self, message: str, details: Optional[Dict] = None) -> bool:
        """Registra warning"""
        self.heartbeat_metrics['warnings'] += 1
        
        logs = {
            'event': 'warning',
            'message': message,
            'details': details or {}
        }
        
        return self._send_heartbeat(status='warning', logs=logs)
    
    # ============================================
    # MÉTODOS ORIGINAIS SUPERBID
    # ============================================
    
    def upsert(self, items: List[Dict]) -> Dict:
        """Upsert de itens na tabela"""
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        print(f"\n📤 Preparando {len(items)} itens para inserção...")
        
        prepared = []
        errors = 0
        for item in items:
            try:
                db_item = self._prepare_item(item)
                if db_item:
                    prepared.append(db_item)
            except Exception as e:
                errors += 1
                if errors <= 5:  # Mostra só primeiros 5 erros
                    print(f"  ⚠️  Erro ao preparar: {str(e)[:100]}")
        
        if not prepared:
            print("  ⚠️  Nenhum item válido para inserir")
            return {'inserted': 0, 'updated': 0, 'errors': errors}
        
        print(f"✅ {len(prepared)} itens preparados ({errors} erros)")
        
        # Insere em batches
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(prepared) + batch_size - 1) // batch_size
        
        url = f"{self.url}/rest/v1/{self.table}"
        
        for i in range(0, len(prepared), batch_size):
            batch = prepared[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(url, json=batch, timeout=120)
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens inseridos")
                    
                    # ✅ HEARTBEAT: Atualiza progresso a cada batch
                    self.heartbeat_progress(
                        items_processed=len(batch),
                        custom_logs={'batch': batch_num, 'total_batches': total_batches}
                    )
                    
                elif r.status_code == 409:
                    stats['updated'] += len(batch)
                    print(f"  🔄 Batch {batch_num}/{total_batches}: {len(batch)} atualizados")
                else:
                    error_detail = r.text[:300] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}: HTTP {r.status_code}")
                    print(f"     {error_detail}")
                    stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ❌ Batch {batch_num}: {str(e)[:100]}")
                stats['errors'] += len(batch)
            
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    def _prepare_item(self, item: Dict) -> Optional[Dict]:
        """Extrai TODOS os campos do raw_data para schema real"""
        external_id = item.get('external_id')
        if not external_id:
            return None
        
        raw = item.get('raw_data', {})
        
        # ==========================================
        # HELPERS
        # ==========================================
        def get(path: str, default=None) -> Any:
            """Extrai valor usando dot notation"""
            keys = path.split('.')
            value = raw
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return default
                if value is None:
                    return default
            return value
        
        def safe_int(val):
            if val is None or val == '':
                return None
            try:
                return int(val)
            except:
                return None
        
        def safe_float(val):
            if val is None or val == '':
                return None
            try:
                return float(val)
            except:
                return None
        
        def safe_bool(val):
            if val is None:
                return False
            if isinstance(val, bool):
                return val
            return str(val).lower() in ('true', '1', 'yes', 'sim')
        
        def safe_str(val):
            if val is None or val == '':
                return None
            return str(val)
        
        def safe_datetime(val):
            if not val:
                return None
            try:
                dt_str = str(val).replace('Z', '+00:00')
                return datetime.fromisoformat(dt_str).isoformat()
            except:
                return None
        
        # ==========================================
        # EXTRAÇÃO DE CAMPOS
        # ==========================================
        
        # IDs (obrigatórios e opcionais)
        offer_id = safe_int(get('id'))
        if not offer_id:
            return None  # offer_id é NOT NULL
        
        product_id = safe_int(get('product.productId'))
        auction_id = safe_int(get('auction.id'))
        lot_number = safe_int(get('lotNumber'))
        group_offer_id = safe_int(get('groupOffer.id'))
        
        # Categoria e Tipo
        category = safe_str(get('product.subCategory.category.description'))
        product_type_id = safe_int(get('product.productType.id'))
        product_type_desc = safe_str(get('product.productType.description'))
        sub_category_id = safe_int(get('product.subCategory.id'))
        sub_category_desc = safe_str(get('product.subCategory.description'))
        
        # Básico
        title = safe_str(get('product.shortDesc', 'Sem Título'))
        if not title:
            title = 'Sem Título'
        
        description = safe_str(get('product.detailedDescription'))
        
        # Localização
        location_city = safe_str(get('product.location.city'))
        location_state = safe_str(get('product.location.state'))
        location_full = location_city
        
        # Monta location_full (usado pelo trigger extract_city_state_superbid)
        if location_city and location_state:
            location_full = f"{location_city} - {location_state}"
        
        # State validado (2 caracteres uppercase)
        state = None
        if location_state:
            state_str = str(location_state).strip().upper()
            if len(state_str) == 2:
                state = state_str
        
        # Coordenadas
        location_lat = safe_float(get('product.location.locationGeo.lat'))
        location_lon = safe_float(get('product.location.locationGeo.lon'))
        
        # Auction
        auction_name = safe_str(get('auction.desc'))
        auction_status_id = safe_int(get('auction.statusId'))
        auction_modality = safe_str(get('auction.modalityDesc'))
        auction_begin_date = safe_datetime(get('auction.beginDate'))
        auction_end_date = safe_datetime(get('auction.endDate'))
        auction_max_enddate = safe_datetime(get('auction.maxEnddateOffer'))
        
        # Auctioneer
        auctioneer_name = safe_str(get('auction.auctioneer'))
        auctioneer_registry = safe_str(get('auction.registry'))
        
        # Auction Address (JSONB)
        auction_address = get('auction.address')
        if not isinstance(auction_address, dict):
            auction_address = None
        
        # Judicial
        judicial_praca = safe_int(get('auction.judicialPraca'))
        judicial_praca_desc = safe_str(get('auction.judicialPracaDescription'))
        judicial_control_number = safe_str(get('auction.judicialControlNumber'))
        
        # Store
        store_id = safe_int(get('store.id'))
        store_name = safe_str(get('store.name'))
        store_highlight = safe_bool(get('store.highlight'))
        store_logo_url = safe_str(get('store.logoUri'))
        
        # Manager
        manager_id = safe_int(get('manager.id'))
        manager_name = safe_str(get('manager.name'))
        
        # Valores
        price = safe_float(get('price'))
        price_formatted = safe_str(get('priceFormatted'))
        initial_bid_value = safe_float(get('offerDetail.initialBidValue'))
        current_min_bid = safe_float(get('offerDetail.currentMinBid'))
        current_max_bid = safe_float(get('offerDetail.currentMaxBid'))
        reserved_price = safe_float(get('offerDetail.reservedPrice'))
        bid_increment = safe_float(get('currentBidIncrement.currentBidIncrement'))
        
        # Lances
        has_bids = safe_bool(get('hasBids'))
        has_received_bids_or_proposals = safe_bool(get('hasReceivedBidsOrProposals'))
        total_bidders = safe_int(get('totalBidders')) or 0
        total_bids = safe_int(get('totalBids')) or 0
        total_received_proposals = safe_int(get('totalReceivedProposals')) or 0
        
        # Winner
        current_winner_id = safe_int(get('winnerBid.userId'))
        current_winner_login = safe_str(get('winnerBid.userLogin'))
        
        # Produto - Veículos (brand e model podem ser dict)
        brand_data = get('product.brand')
        if isinstance(brand_data, dict):
            brand = safe_str(brand_data.get('description'))
        else:
            brand = safe_str(brand_data)
        
        model_data = get('product.model')
        if isinstance(model_data, dict):
            model = safe_str(model_data.get('description'))
        else:
            model = safe_str(model_data)
        
        # Extrai características do template
        year_manufacture = None
        year_model = None
        plate = None
        color = None
        fuel = None
        transmission = None
        km = None
        vehicle_restrictions = None
        vehicle_owner = None
        vehicle_debts = None
        
        template = get('product.template', {})
        if isinstance(template, dict):
            for group in template.get('groups', []):
                for prop in group.get('properties', []):
                    prop_id = str(prop.get('id', '')).lower()
                    value = prop.get('value')
                    
                    if not value:
                        continue
                    
                    # Mapeamento de campos
                    if prop_id == 'anofabricacao':
                        year_manufacture = safe_int(value)
                    elif prop_id == 'anomodelo':
                        year_model = safe_int(value)
                    elif prop_id == 'placa':
                        plate = safe_str(value)
                    elif prop_id == 'cor':
                        color = safe_str(value)
                    elif prop_id == 'combustivel':
                        fuel = safe_str(value)
                    elif prop_id == 'cambio':
                        transmission = safe_str(value)
                    elif prop_id in ('km', 'quilometragem'):
                        km = safe_int(value)
                    elif prop_id in ('restricoes', 'restricao'):
                        vehicle_restrictions = safe_str(value)
                    elif prop_id in ('proprietario', 'dono'):
                        vehicle_owner = safe_str(value)
                    elif prop_id in ('debitos', 'dividas'):
                        vehicle_debts = safe_str(value)
        
        # Product ref
        product_your_ref = safe_str(get('product.productYourRef'))
        
        # Imagens
        image_url = safe_str(get('product.thumbnailUrl'))
        photo_count = safe_int(get('product.photoCount')) or 0
        video_url_count = safe_int(get('product.videoUrlCount')) or 0
        
        # Status Offer
        offer_status_id = safe_int(get('statusId'))
        offer_type_id = safe_int(get('offerTypeId'))
        status_code = safe_int(get('offerStatus.statusCode'))
        is_removed = safe_bool(get('offerStatus.removed'))
        is_stabbed = safe_bool(get('offerStatus.stabbed'))
        is_subjudice = safe_bool(get('offerStatus.subjudice'))
        is_sold = safe_bool(get('offerStatus.sold'))
        is_reserved = safe_bool(get('offerStatus.reserved'))
        is_closed = safe_bool(get('offerStatus.closed'))
        is_highlight = safe_bool(get('store.highlight'))
        is_favorite = safe_bool(get('isFavorite'))
        
        # Quantidades
        quantity_in_lot = safe_int(get('quantityInLot')) or 1
        quantity_sold = safe_int(get('quantitySold')) or 0
        quantity_reserved = safe_int(get('quantityReserved')) or 0
        
        # Métricas
        system_metric = safe_str(get('systemMetric'))
        visits = safe_int(get('visits')) or 0
        
        # Seller
        seller_id = safe_int(get('seller.id'))
        seller_name = safe_str(get('seller.name'))
        seller_city = safe_str(get('seller.city'))
        
        # Seller phone (JSONB)
        seller_phone = get('seller.phone')
        if not isinstance(seller_phone, (dict, list)):
            seller_phone = None
        
        # Seller company (JSONB)
        seller_company = get('seller.company')
        if not isinstance(seller_company, dict):
            seller_company = None
        
        # Commercial
        commission_percent = safe_float(get('groupOffer.commissionPercent'))
        allows_credit_card = safe_bool(get('commercialCondition.allowsCreditCard'))
        allows_credit_card_total = safe_bool(get('commercialCondition.allowCreditCardTotalValue'))
        transaction_limit = safe_float(get('commercialCondition.transactionLimit'))
        max_installments = safe_int(get('commercialCondition.maxInstallments'))
        
        # Datas
        end_date = safe_datetime(get('endDate'))
        end_date_time = safe_int(get('endDateTime'))
        create_at = safe_datetime(get('createAt'))
        update_at = safe_datetime(get('updateAt'))
        published_at = safe_datetime(get('publishedAt'))
        indexation_date = safe_datetime(get('indexationDate'))
        
        # Link (obrigatório)
        link = item.get('link')
        if not link:
            link = f"https://exchange.superbid.net/oferta/{offer_id}"
        
        # ==========================================
        # METADATA (dados adicionais e complexos)
        # ==========================================
        metadata = {
            'category_display': item.get('category_display'),
            'scraped_at': item.get('scraped_at'),
        }
        
        # Gallery (fotos)
        gallery = get('product.galleryJson')
        if gallery:
            metadata['gallery'] = gallery
        
        # Template completo
        if template:
            metadata['template'] = template
        
        # Product custom JSON
        product_custom = get('product.productCustomJson')
        if product_custom:
            metadata['product_custom'] = product_custom
        
        # SubMarketplaces
        sub_marketplaces = get('auction.subMarketplaces')
        if sub_marketplaces:
            metadata['sub_marketplaces'] = sub_marketplaces
        
        # EventPipeline
        event_pipeline = get('auction.eventPipeline')
        if event_pipeline:
            metadata['event_pipeline'] = event_pipeline
        
        # Stores array
        stores = get('stores')
        if stores:
            metadata['stores'] = stores
        
        # Winner bid
        winner_bid = get('winnerBid')
        if winner_bid:
            metadata['winner_bid'] = winner_bid
        
        # ==========================================
        # RETORNO (todos os campos do schema)
        # ==========================================
        return {
            'external_id': external_id,
            'offer_id': offer_id,
            'product_id': product_id,
            'lot_number': lot_number,
            'auction_id': auction_id,
            'group_offer_id': group_offer_id,
            
            'category': category,
            'product_type_id': product_type_id,
            'product_type_desc': product_type_desc,
            'sub_category_id': sub_category_id,
            'sub_category_desc': sub_category_desc,
            
            'title': title,
            'description': description,
            
            'city': location_city,
            'state': state,
            'location_full': location_full,
            'location_lat': location_lat,
            'location_lon': location_lon,
            
            'auction_name': auction_name,
            'auction_status_id': auction_status_id,
            'auction_modality': auction_modality,
            'auction_begin_date': auction_begin_date,
            'auction_end_date': auction_end_date,
            'auction_max_enddate': auction_max_enddate,
            
            'auctioneer_name': auctioneer_name,
            'auctioneer_registry': auctioneer_registry,
            'auction_address': auction_address,
            
            'judicial_praca': judicial_praca,
            'judicial_praca_desc': judicial_praca_desc,
            'judicial_control_number': judicial_control_number,
            
            'store_id': store_id,
            'store_name': store_name,
            'store_highlight': store_highlight,
            'store_logo_url': store_logo_url,
            
            'manager_id': manager_id,
            'manager_name': manager_name,
            
            'price': price,
            'price_formatted': price_formatted,
            'initial_bid_value': initial_bid_value,
            'current_min_bid': current_min_bid,
            'current_max_bid': current_max_bid,
            'reserved_price': reserved_price,
            'bid_increment': bid_increment,
            
            'has_bids': has_bids,
            'has_received_bids_or_proposals': has_received_bids_or_proposals,
            'total_bidders': total_bidders,
            'total_bids': total_bids,
            'total_received_proposals': total_received_proposals,
            
            'current_winner_id': current_winner_id,
            'current_winner_login': current_winner_login,
            
            'brand': brand,
            'model': model,
            'year_manufacture': year_manufacture,
            'year_model': year_model,
            'plate': plate,
            'color': color,
            'fuel': fuel,
            'transmission': transmission,
            'km': km,
            'vehicle_restrictions': vehicle_restrictions,
            'vehicle_owner': vehicle_owner,
            'vehicle_debts': vehicle_debts,
            'product_your_ref': product_your_ref,
            
            'image_url': image_url,
            'photo_count': photo_count,
            'video_url_count': video_url_count,
            
            'offer_status_id': offer_status_id,
            'offer_type_id': offer_type_id,
            'status_code': status_code,
            'is_removed': is_removed,
            'is_stabbed': is_stabbed,
            'is_subjudice': is_subjudice,
            'is_sold': is_sold,
            'is_reserved': is_reserved,
            'is_closed': is_closed,
            'is_highlight': is_highlight,
            'is_favorite': is_favorite,
            
            'quantity_in_lot': quantity_in_lot,
            'quantity_sold': quantity_sold,
            'quantity_reserved': quantity_reserved,
            
            'system_metric': system_metric,
            'visits': visits,
            
            'seller_id': seller_id,
            'seller_name': seller_name,
            'seller_city': seller_city,
            'seller_phone': seller_phone,
            'seller_company': seller_company,
            
            'commission_percent': commission_percent,
            'allows_credit_card': allows_credit_card,
            'allows_credit_card_total': allows_credit_card_total,
            'transaction_limit': transaction_limit,
            'max_installments': max_installments,
            
            'end_date': end_date,
            'end_date_time': end_date_time,
            'create_at': create_at,
            'update_at': update_at,
            'published_at': published_at,
            'indexation_date': indexation_date,
            
            'link': link,
            'source': 'superbid',
            'metadata': metadata,
            'is_active': True,
            'has_bid': has_bids,
            'last_scraped_at': datetime.now().isoformat(),
        }
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


if __name__ == "__main__":
    print("🧪 SupabaseSuperbid Client com Heartbeat")
    client = SupabaseSuperbid(service_name='test_superbid')
    print("✅ Cliente OK")
    
    # Teste heartbeat
    print("\n💓 Testando Heartbeat:")
    client.heartbeat_start({'test': True})
    time.sleep(1)
    client.heartbeat_progress(items_processed=10, categories_scraped=1)
    time.sleep(1)
    client.heartbeat_success({'total': 10})
    print("\n✅ Heartbeat funcionando!")