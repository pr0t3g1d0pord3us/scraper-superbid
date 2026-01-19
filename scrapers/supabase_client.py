#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - SUPERBID_ITEMS
✅ Cliente específico para tabela superbid_items
✅ Suporta todos os campos complexos da tabela
✅ Validação de dados conforme schema
"""

import os
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional


class SupabaseSuperbid:
    """Cliente Supabase para tabela superbid_items"""
    
    def __init__(self):
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
    
    def upsert(self, items: List[Dict]) -> Dict:
        """Upsert de itens na tabela superbid_items"""
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Prepara itens
        prepared = []
        for item in items:
            try:
                db_item = self._prepare_item(item)
                if db_item:
                    prepared.append(db_item)
            except Exception as e:
                print(f"  ⚠️ Erro ao preparar item: {e}")
        
        if not prepared:
            print("  ⚠️ Nenhum item válido para inserir")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
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
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens")
                elif r.status_code == 409:
                    stats['updated'] += len(batch)
                    print(f"  🔄 Batch {batch_num}/{total_batches}: {len(batch)} atualizados")
                else:
                    error_msg = r.text[:200] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}: HTTP {r.status_code} - {error_msg}")
                    stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ❌ Batch {batch_num}: {e}")
                stats['errors'] += len(batch)
            
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    def _prepare_item(self, item: Dict) -> Optional[Dict]:
        """Prepara item para inserção validando campos"""
        external_id = item.get('external_id')
        if not external_id:
            return None
        
        # Valida state
        state = item.get('state')
        if state:
            state = str(state).strip().upper()
            if len(state) != 2:
                state = None
        
        # Converte valores numéricos
        def safe_float(value):
            if value is None:
                return None
            try:
                return float(value)
            except:
                return None
        
        def safe_int(value):
            if value is None:
                return None
            try:
                return int(value)
            except:
                return None
        
        def safe_bool(value):
            if value is None:
                return False
            if isinstance(value, bool):
                return value
            return str(value).lower() in ('true', '1', 'yes', 'sim')
        
        # Processa datas
        def safe_datetime(value):
            if not value:
                return None
            if isinstance(value, str):
                try:
                    value = value.replace('Z', '+00:00')
                    dt = datetime.fromisoformat(value)
                    return dt.isoformat()
                except:
                    return None
            return None
        
        # Processa metadata
        metadata = item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        
        # Processa JSONs
        seller_phone = item.get('seller_phone')
        if seller_phone and not isinstance(seller_phone, list):
            seller_phone = None
        
        seller_company = item.get('seller_company')
        if seller_company and not isinstance(seller_company, list):
            seller_company = None
        
        auction_address = item.get('auction_address')
        if auction_address and not isinstance(auction_address, dict):
            auction_address = None
        
        # Monta item com todos os campos
        data = {
            'external_id': str(external_id),
            'offer_id': safe_int(item.get('offer_id')),
            'product_id': safe_int(item.get('product_id')),
            'lot_number': safe_int(item.get('lot_number')),
            'auction_id': safe_int(item.get('auction_id')),
            'group_offer_id': safe_int(item.get('group_offer_id')),
            'category': str(item.get('category')) if item.get('category') else None,
            'product_type_id': safe_int(item.get('product_type_id')),
            'product_type_desc': str(item.get('product_type_desc')) if item.get('product_type_desc') else None,
            'sub_category_id': safe_int(item.get('sub_category_id')),
            'sub_category_desc': str(item.get('sub_category_desc')) if item.get('sub_category_desc') else None,
            'title': str(item.get('title', 'Sem Título')),
            'description': str(item.get('description')) if item.get('description') else None,
            'city': str(item.get('city')) if item.get('city') else None,
            'state': state,
            'location_full': str(item.get('location_full')) if item.get('location_full') else None,
            'location_lat': safe_float(item.get('location_lat')),
            'location_lon': safe_float(item.get('location_lon')),
            'auction_name': str(item.get('auction_name')) if item.get('auction_name') else None,
            'auction_status_id': safe_int(item.get('auction_status_id')),
            'auction_modality': str(item.get('auction_modality')) if item.get('auction_modality') else None,
            'auction_begin_date': safe_datetime(item.get('auction_begin_date')),
            'auction_end_date': safe_datetime(item.get('auction_end_date')),
            'auction_max_enddate': safe_datetime(item.get('auction_max_enddate')),
            'auctioneer_name': str(item.get('auctioneer_name')) if item.get('auctioneer_name') else None,
            'auctioneer_registry': str(item.get('auctioneer_registry')) if item.get('auctioneer_registry') else None,
            'auction_address': auction_address,
            'judicial_praca': safe_int(item.get('judicial_praca')),
            'judicial_praca_desc': str(item.get('judicial_praca_desc')) if item.get('judicial_praca_desc') else None,
            'judicial_control_number': str(item.get('judicial_control_number')) if item.get('judicial_control_number') else None,
            'store_id': safe_int(item.get('store_id')),
            'store_name': str(item.get('store_name')) if item.get('store_name') else None,
            'store_highlight': safe_bool(item.get('store_highlight')),
            'store_logo_url': str(item.get('store_logo_url')) if item.get('store_logo_url') else None,
            'manager_id': safe_int(item.get('manager_id')),
            'manager_name': str(item.get('manager_name')) if item.get('manager_name') else None,
            'price': safe_float(item.get('price')),
            'price_formatted': str(item.get('price_formatted')) if item.get('price_formatted') else None,
            'initial_bid_value': safe_float(item.get('initial_bid_value')),
            'current_min_bid': safe_float(item.get('current_min_bid')),
            'current_max_bid': safe_float(item.get('current_max_bid')),
            'reserved_price': safe_float(item.get('reserved_price')),
            'bid_increment': safe_float(item.get('bid_increment')),
            'has_bids': safe_bool(item.get('has_bids')),
            'has_received_bids_or_proposals': safe_bool(item.get('has_received_bids_or_proposals')),
            'total_bidders': safe_int(item.get('total_bidders')) or 0,
            'total_bids': safe_int(item.get('total_bids')) or 0,
            'total_received_proposals': safe_int(item.get('total_received_proposals')) or 0,
            'current_winner_id': safe_int(item.get('current_winner_id')),
            'current_winner_login': str(item.get('current_winner_login')) if item.get('current_winner_login') else None,
            'brand': str(item.get('brand')) if item.get('brand') else None,
            'model': str(item.get('model')) if item.get('model') else None,
            'year_manufacture': safe_int(item.get('year_manufacture')),
            'year_model': safe_int(item.get('year_model')),
            'plate': str(item.get('plate')) if item.get('plate') else None,
            'color': str(item.get('color')) if item.get('color') else None,
            'fuel': str(item.get('fuel')) if item.get('fuel') else None,
            'transmission': str(item.get('transmission')) if item.get('transmission') else None,
            'km': safe_int(item.get('km')),
            'vehicle_restrictions': str(item.get('vehicle_restrictions')) if item.get('vehicle_restrictions') else None,
            'vehicle_owner': str(item.get('vehicle_owner')) if item.get('vehicle_owner') else None,
            'vehicle_debts': str(item.get('vehicle_debts')) if item.get('vehicle_debts') else None,
            'product_your_ref': str(item.get('product_your_ref')) if item.get('product_your_ref') else None,
            'image_url': str(item.get('image_url')) if item.get('image_url') else None,
            'photo_count': safe_int(item.get('photo_count')) or 0,
            'video_url_count': safe_int(item.get('video_url_count')) or 0,
            'offer_status_id': safe_int(item.get('offer_status_id')),
            'offer_type_id': safe_int(item.get('offer_type_id')),
            'status_code': safe_int(item.get('status_code')),
            'is_removed': safe_bool(item.get('is_removed')),
            'is_stabbed': safe_bool(item.get('is_stabbed')),
            'is_subjudice': safe_bool(item.get('is_subjudice')),
            'is_sold': safe_bool(item.get('is_sold')),
            'is_reserved': safe_bool(item.get('is_reserved')),
            'is_closed': safe_bool(item.get('is_closed')),
            'is_highlight': safe_bool(item.get('is_highlight')),
            'is_favorite': safe_bool(item.get('is_favorite')),
            'quantity_in_lot': safe_int(item.get('quantity_in_lot')) or 1,
            'quantity_sold': safe_int(item.get('quantity_sold')) or 0,
            'quantity_reserved': safe_int(item.get('quantity_reserved')) or 0,
            'system_metric': str(item.get('system_metric')) if item.get('system_metric') else None,
            'visits': safe_int(item.get('visits')) or 0,
            'seller_id': safe_int(item.get('seller_id')),
            'seller_name': str(item.get('seller_name')) if item.get('seller_name') else None,
            'seller_city': str(item.get('seller_city')) if item.get('seller_city') else None,
            'seller_phone': seller_phone,
            'seller_company': seller_company,
            'commission_percent': safe_float(item.get('commission_percent')),
            'allows_credit_card': safe_bool(item.get('allows_credit_card')),
            'allows_credit_card_total': safe_bool(item.get('allows_credit_card_total')),
            'transaction_limit': safe_float(item.get('transaction_limit')),
            'max_installments': safe_int(item.get('max_installments')),
            'end_date': safe_datetime(item.get('end_date')),
            'end_date_time': safe_int(item.get('end_date_time')),
            'create_at': safe_datetime(item.get('create_at')),
            'update_at': safe_datetime(item.get('update_at')),
            'published_at': safe_datetime(item.get('published_at')),
            'indexation_date': safe_datetime(item.get('indexation_date')),
            'link': str(item.get('link')) if item.get('link') else None,
            'source': str(item.get('source', 'superbid')),
            'metadata': metadata,
            'is_active': True,
            'has_bid': safe_bool(item.get('has_bid')),
            'last_scraped_at': datetime.now().isoformat(),
        }
        
        return data
    
    def test(self) -> bool:
        """Testa conexão com Supabase"""
        try:
            url = f"{self.url}/rest/v1/"
            r = self.session.get(url, timeout=10)
            
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code}")
                return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas da tabela"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0').split('/')[-1])
                return {'total': total, 'table': self.table}
        except:
            pass
        
        return {'total': 0, 'table': self.table}
    
    def get_by_category(self, category: str, limit: int = 100) -> List[Dict]:
        """Busca itens por categoria"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            params = {
                'category': f'eq.{category}',
                'is_active': 'eq.true',
                'order': 'created_at.desc',
                'limit': limit
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                return r.json()
        except:
            pass
        
        return []
    
    def get_with_bids(self, limit: int = 100) -> List[Dict]:
        """Busca itens com lances"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            params = {
                'has_bids': 'eq.true',
                'is_active': 'eq.true',
                'order': 'total_bids.desc',
                'limit': limit
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                return r.json()
        except:
            pass
        
        return []
    
    def get_by_store(self, store_name: str, limit: int = 100) -> List[Dict]:
        """Busca itens por loja"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            params = {
                'store_name': f'eq.{store_name}',
                'is_active': 'eq.true',
                'order': 'created_at.desc',
                'limit': limit
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                return r.json()
        except:
            pass
        
        return []
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


if __name__ == "__main__":
    # Teste do cliente
    print("🧪 Testando SupabaseSuperbid\n")
    
    client = SupabaseSuperbid()
    
    if client.test():
        stats = client.get_stats()
        print(f"\n📊 Estatísticas:")
        print(f"  Total de itens: {stats['total']}")
        
        # Exemplo de busca por categoria
        carros = client.get_by_category('Carros', limit=5)
        print(f"\n🚗 Primeiros carros: {len(carros)} itens")
        
        # Exemplo de busca com lances
        com_lances = client.get_with_bids(limit=5)
        print(f"💰 Com lances: {len(com_lances)} itens")
        
        # Exemplo de busca por loja
        sold = client.get_by_store('SOLD', limit=5)
        print(f"🏪 Loja SOLD: {len(sold)} itens")